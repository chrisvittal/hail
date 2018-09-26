package is.hail.rvd

import is.hail.annotations._
import is.hail.sparkextras._
import is.hail.utils.fatal

import scala.collection.generic.Growable

class KeyedRVD(val rvd: RVD, val key: Int) {
  require(key <= rvd.typ.key.length && key >= 0)
  val realType: RVDType = rvd.typ
  val virtType = RVDType(realType.rowType, realType.key.take(key))
  val (kType, _) = rvd.rowType.select(virtType.key)

  private def checkJoinCompatability(right: KeyedRVD) {
    if (!(kType isIsomorphicTo right.kType))
      fatal(
        s"""Incompatible join keys. Keys must have same length and types, in order:
           | Left join key type: ${ kType.toString }
           | Right join key type: ${ right.kType.toString }
         """.stripMargin)
  }

  // 'joinedType.key' must be the join key, followed by the remaining left key,
  // followed by the (possibly renamed) remaining right key. 'joiner' must copy
  // these 'joinedType.key' fields from the corresponding fields in the
  // JoinedRegionValue.
  def orderedJoin(
    right: KeyedRVD,
    joinType: String,
    joiner: (RVDContext, Iterator[JoinedRegionValue]) => Iterator[RegionValue],
    joinedType: RVDType
  ): RVD = {
    checkJoinCompatability(right)

    val newPartitioner = {
      def leftPart = this.rvd.partitioner.strictify
      def rightPart = right.rvd.partitioner.coarsen(key).extendKey(realType.kType)
      (joinType: @unchecked) match {
        case "left" => leftPart
        case "right" => rightPart
        case "inner" => leftPart.intersect(rightPart)
        case "outer" => RVDPartitioner.generate(
          realType.kType,
          leftPart.rangeBounds ++ rightPart.rangeBounds)
      }
    }
    val repartitionedLeft = rvd.repartition(newPartitioner)
    val compute: (OrderedRVIterator, OrderedRVIterator, Iterable[RegionValue] with Growable[RegionValue]) => Iterator[JoinedRegionValue] =
      (joinType: @unchecked) match {
        case "inner" => _.innerJoin(_, _)
        case "left" => _.leftJoin(_, _)
        case "right" => _.rightJoin(_, _)
        case "outer" => _.outerJoin(_, _)
      }
    val lTyp = virtType
    val rTyp = right.virtType
    val rRowType = right.realType.rowType

    repartitionedLeft.alignAndZipPartitions(
      joinedType.copy(key = joinedType.key.take(realType.key.length)),
      right.rvd,
      key
    ) { (ctx, leftIt, rightIt) =>
      val sideBuffer = ctx.freshContext.region
      joiner(
        ctx,
        compute(
          OrderedRVIterator(lTyp, leftIt, ctx),
          OrderedRVIterator(rTyp, rightIt, ctx),
          new RegionValueArrayBuffer(rRowType, sideBuffer)))
    }.extendKeyPreservesPartitioning(joinedType.key)
  }

  def orderedJoinDistinct(
    right: KeyedRVD,
    joinType: String,
    joiner: (RVDContext, Iterator[JoinedRegionValue]) => Iterator[RegionValue],
    joinedType: RVDType
  ): RVD = {
    checkJoinCompatability(right)
    val lTyp = virtType
    val rTyp = right.virtType

    val compute: (OrderedRVIterator, OrderedRVIterator) => Iterator[JoinedRegionValue] =
      (joinType: @unchecked) match {
        case "inner" => _.innerJoinDistinct(_)
        case "left" => _.leftJoinDistinct(_)
      }

    rvd.alignAndZipPartitions(
      joinedType,
      right.rvd,
      key
    ) { (ctx, leftIt, rightIt) =>
      joiner(
        ctx,
        compute(
          OrderedRVIterator(lTyp, leftIt, ctx),
          OrderedRVIterator(rTyp, rightIt, ctx)))
    }
  }

  def orderedZipJoin(right: KeyedRVD): ContextRDD[RVDContext, JoinedRegionValue] = {
    checkJoinCompatability(right)
    val ranges = this.rvd.partitioner.coarsenedRangeBounds(key) ++
      right.rvd.partitioner.coarsenedRangeBounds(key)
    val newPartitioner = RVDPartitioner.generate(virtType.key, kType, ranges)

    val repartitionedLeft = this.rvd.repartition(newPartitioner)
    val repartitionedRight = right.rvd.repartition(newPartitioner)

    val leftType = this.virtType
    val rightType = right.virtType
    repartitionedLeft.crddBoundary.czipPartitions(repartitionedRight.crddBoundary)
      { (ctx, leftIt, rightIt) =>
        OrderedRVIterator(leftType, leftIt, ctx)
          .zipJoin(OrderedRVIterator(rightType, rightIt, ctx))
      }
  }

  def orderedMerge(right: KeyedRVD): RVD = {
    checkJoinCompatability(right)
    require(this.realType.rowType == right.realType.rowType)

    if (key == 0)
      return RVD.unkeyed(
        this.realType.rowType,
        ContextRDD.union(
          rvd.sparkContext,
          Seq(this.rvd.crdd, right.rvd.crdd)))

    val ranges = this.rvd.partitioner.coarsenedRangeBounds(key) ++
      right.rvd.partitioner.coarsenedRangeBounds(key)
    val newPartitioner = RVDPartitioner.generate(virtType.key, kType, ranges)

    val repartitionedLeft =
      this.rvd.repartition(newPartitioner)
    val lType = this.virtType
    val rType = right.virtType

    repartitionedLeft.alignAndZipPartitions(
      this.virtType,
      right.rvd,
      key
    ) { (ctx, leftIt, rightIt) =>
      OrderedRVIterator(lType, leftIt, ctx)
        .merge(OrderedRVIterator(rType, rightIt, ctx))
    }
  }
}
