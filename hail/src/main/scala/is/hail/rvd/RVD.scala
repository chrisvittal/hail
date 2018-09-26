package is.hail.rvd

import java.util

import is.hail.HailContext
import is.hail.annotations._
import is.hail.expr.JSONAnnotationImpex
import is.hail.expr.ir.PruneDeadFields.isSupertype
import is.hail.expr.types._
import is.hail.expr.types.physical.PInt64
import is.hail.io.CodecSpec
import is.hail.sparkextras._
import is.hail.utils._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel

import scala.language.existentials
import scala.reflect.ClassTag

abstract class RVDCoercer(val fullType: RVDType) {
  final def coerce(typ: RVDType, crdd: ContextRDD[RVDContext, RegionValue]): RVD = {
    require(isSupertype(typ.rowType, fullType.rowType))
    require(typ.key.sameElements(fullType.key))
    _coerce(typ, crdd)
  }

  protected def _coerce(typ: RVDType, crdd: ContextRDD[RVDContext, RegionValue]): RVD
}

class RVD(
  val typ: RVDType,
  val partitioner: RVDPartitioner,
  val crdd: ContextRDD[RVDContext, RegionValue]
) {
  self =>
  require(crdd.getNumPartitions == partitioner.numPartitions)

  require(typ.kType isIsomorphicTo partitioner.kType)

  // Basic accessors

  def sparkContext: SparkContext = crdd.sparkContext

  def getNumPartitions: Int = crdd.getNumPartitions

  def rowType: TStruct = typ.rowType

  def boundary: RVD = RVD(typ, partitioner, crddBoundary)

  // Basic manipulators

  def cast(newRowType: TStruct): RVD = {
    val nameMap = rowType.fieldNames.zip(newRowType.fieldNames).toMap
    val newTyp = RVDType(newRowType, typ.key.map(nameMap))
    val newPartitioner = partitioner.rename(nameMap)
    new RVD(newTyp, newPartitioner, crdd)
  }

  // Change the row type to a physically equivalent type.
  def changeType(newRowType: TStruct): RVD = {
    require(typ.key.forall(newRowType.fieldNames.contains))
    copy(typ = typ.copy(rowType = newRowType))
  }

  // Exporting

  def toRows: RDD[Row] = {
    val localRowType = rowType
    map(rv => SafeRow(localRowType.physicalType, rv.region, rv.offset))
  }

  def stabilize(codec: CodecSpec = RVD.memoryCodec): RDD[Array[Byte]] = {
    val enc = codec.buildEncoder(rowType)
    crdd.mapPartitions(_.map(_.toBytes(enc))).clearingRun
  }

  def encodedRDD(codec: CodecSpec): RDD[Array[Byte]] =
    stabilize(codec)

  def keyedEncodedRDD(codec: CodecSpec, key: IndexedSeq[String] = typ.key): RDD[(Any, Array[Byte])] = {
    val enc = codec.buildEncoder(rowType)
    val kFieldIdx = typ.copy(key = key).kFieldIdx
    val rowPType = rowType.physicalType

    crdd.mapPartitions { it =>
      it.map { rv =>
        val keys: Any = SafeRow.selectFields(rowPType, rv)(kFieldIdx)
        val bytes = rv.toBytes(enc)
        (keys, bytes)
      }
    }.clearingRun
  }

  // Key and partitioner manipulation

  def changeKey(newKey: IndexedSeq[String]): RVD =
    changeKey(newKey, newKey.length)

  def changeKey(newKey: IndexedSeq[String], partitionKey: Int): RVD =
    RVD.coerce(typ.copy(key = newKey), partitionKey, this.crdd)

  def extendKeyPreservesPartitioning(newKey: IndexedSeq[String]): RVD = {
    require(newKey startsWith typ.key)
    require(newKey.forall(typ.rowType.fieldNames.contains))
    val rvdType = typ.copy(key = newKey)
    if (RVDPartitioner.isValid(rvdType.kType, partitioner.rangeBounds))
      copy(typ = rvdType, partitioner = partitioner.copy(kType = rvdType.kType))
    else {
      val adjustedPartitioner = partitioner.strictify
      repartition(adjustedPartitioner)
        .copy(typ = rvdType, partitioner = adjustedPartitioner.copy(kType = rvdType.kType))
    }
  }

  def truncateKey(newKey: IndexedSeq[String]): RVD = {
    require(typ.key startsWith newKey)
    copy(
      typ = typ.copy(key = newKey),
      partitioner = partitioner.coarsen(newKey.length))
  }

  // WARNING: will drop any data with keys falling outside 'partitioner'.
  def repartition(
    newPartitioner: RVDPartitioner,
    shuffle: Boolean = false
  ): RVD = {
    require(newPartitioner.satisfiesAllowedOverlap(newPartitioner.kType.size - 1))
    require(shuffle || newPartitioner.kType.isPrefixOf(typ.kType))

    if (shuffle) {
      val newType = typ.copy(key = newPartitioner.kType.fieldNames)

      val rowPType = rowType.physicalType
      val kOrdering = newType.kType.ordering

      val partBc = newPartitioner.broadcast(crdd.sparkContext)
      val codec = RVD.wireCodec

      val filtered: RVD = filterWithContext[(UnsafeRow, KeyedRow)]( { case (_, _) =>
        val ur = new UnsafeRow(rowPType, null, 0)
        val key = new KeyedRow(ur, newType.kFieldIdx)
        (ur, key)
      }, { case ((ur, key), rv) =>
        ur.set(rv)
        partBc.value.rangeTree.contains(kOrdering, key)
      })

      val shuffled: RDD[(Any, Array[Byte])] = new ShuffledRDD(
        filtered.keyedEncodedRDD(codec, newType.key),
        newPartitioner.sparkPartitioner(crdd.sparkContext)
      ).setKeyOrdering(kOrdering.toOrdering)

      val shuffledCRDD = codec.decodeRDD(typ.rowType, shuffled.map(_._2))

      RVD(newType, newPartitioner, shuffledCRDD)

    } else
      new RVD(
        typ.copy(key = typ.key.take(newPartitioner.kType.size)),
        newPartitioner,
        RepartitionedOrderedRDD2(this, newPartitioner.rangeBounds))
  }

  def naiveCoalesce(maxPartitions: Int): RVD = {
    val n = partitioner.numPartitions
    if (maxPartitions >= n)
      return this

    val newN = maxPartitions
    val newNParts = partition(n, newN)
    assert(newNParts.forall(_ > 0))
    val newPartitioner = partitioner.coalesceRangeBounds(newNParts.scanLeft(-1)(_ + _).tail)
    repartition(newPartitioner)
  }

  def coalesce(maxPartitions: Int, shuffle: Boolean): RVD = {
    require(maxPartitions > 0, "cannot coalesce to nPartitions <= 0")
    val n = crdd.partitions.length
    if (!shuffle && maxPartitions >= n)
      return this

    val newPartitioner = if (shuffle) {
      val shuffled = stably(_.coalesce(maxPartitions, shuffle = true))
      val keyInfo = RVD.getKeyInfo(typ, typ.key.length, RVD.getKeys(typ, shuffled))
      if (keyInfo.isEmpty)
        return RVD.empty(sparkContext, typ)
      RVD.calculateKeyRanges(
        typ, keyInfo, shuffled.getNumPartitions, typ.key.length)
    } else {

      val partSize = countPerPartition()
      log.info(s"partSize = ${ partSize.toSeq }")

      val partCumulativeSize = mapAccumulate[Array, Long](partSize, 0L)((s, acc) => (s + acc, s + acc))
      val totalSize = partCumulativeSize.last

      var newPartEnd = (0 until maxPartitions).map { i =>
        val t = totalSize * (i + 1) / maxPartitions

        /* j largest index not greater than t */
        var j = util.Arrays.binarySearch(partCumulativeSize, t)
        if (j < 0)
          j = -j - 1
        while (j < partCumulativeSize.length - 1
          && partCumulativeSize(j + 1) == t)
          j += 1
        assert(t <= partCumulativeSize(j) &&
          (j == partCumulativeSize.length - 1 ||
            t < partCumulativeSize(j + 1)))
        j
      }.toArray

      newPartEnd = newPartEnd.zipWithIndex.filter { case (end, i) => i == 0 || newPartEnd(i) != newPartEnd(i - 1) }
        .map(_._1)

      if (newPartEnd.length < maxPartitions)
        warn(s"coalesced to ${ newPartEnd.length } ${ plural(newPartEnd.length, "partition") }, less than requested $maxPartitions")

      partitioner.coalesceRangeBounds(newPartEnd)
    }

    repartition(newPartitioner, shuffle)
  }

  // Key-wise operations

  def groupByKey(valuesField: String = "values"): RVD = {
    val newTyp = RVDType(
      typ.kType ++ TStruct(valuesField -> TArray(typ.valueType)),
      typ.key)
    val newRowType = newTyp.rowType

    val localType = typ

    RVD(newTyp, partitioner, crdd.cmapPartitionsAndContext { (consumerCtx, useCtxes) =>
      val consumerRegion = consumerCtx.region
      val rvb = consumerCtx.rvb
      val outRV = RegionValue(consumerRegion)

      val bufferRegion = consumerCtx.freshContext.region
      val buffer = new RegionValueArrayBuffer(localType.valueType, bufferRegion)

      val producerCtx = consumerCtx.freshContext
      val producerRegion = producerCtx.region
      val it = useCtxes.flatMap(_ (producerCtx))

      val stepped = OrderedRVIterator(
        localType,
        it,
        consumerCtx.freshContext
      ).staircase

      stepped.map { stepIt =>
        buffer.clear()
        rvb.start(newRowType)
        rvb.startStruct()
        var i = 0
        while (i < localType.kType.size) {
          rvb.addField(localType.rowType, stepIt.value, localType.kFieldIdx(i))
          i += 1
        }
        for (rv <- stepIt) {
          buffer.appendSelect(localType.rowType, localType.valueFieldIdx, rv)
          producerRegion.clear()
        }
        rvb.startArray(buffer.length)
        for (rv <- buffer)
          rvb.addRegionValue(localType.valueType, rv)
        rvb.endArray()
        rvb.endStruct()
        outRV.setOffset(rvb.end())
        outRV
      }
    })
  }

  def distinctByKey(): RVD = {
    val localType = typ
    repartition(partitioner.strictify)
      .mapPartitions(typ, (ctx, it) =>
        OrderedRVIterator(localType, it, ctx)
          .staircase
          .map(_.value)
      )
  }

  def localSort(newKey: IndexedSeq[String]): RVD = {
    require(newKey startsWith typ.key)
    require(newKey.forall(typ.rowType.fieldNames.contains))
    require(partitioner.satisfiesAllowedOverlap(typ.key.length - 1))

    val localTyp = typ
    val sortedRDD = boundary.crdd.cmapPartitions { (consumerCtx, it) =>
      OrderedRVIterator(localTyp, it, consumerCtx).localKeySort(newKey)
    }

    val newType = typ.copy(key = newKey)
    new RVD(
      newType,
      partitioner.copy(kType = newType.kType),
      sortedRDD)
  }

  // Mapping

  // None of the mapping methods may modify values of key fields, so that the
  // partitioner remains valid.

  def map[T](f: (RegionValue) => T)(implicit tct: ClassTag[T]): RDD[T] =
    crdd.map(f).clearingRun

  def map(newTyp: RVDType)(f: (RegionValue) => RegionValue): RVD = {
    require(newTyp.kType isPrefixOf typ.kType)
    RVD(newTyp,
      partitioner.coarsen(newTyp.key.length),
      crdd.map(f))
  }

  def mapPartitions[T: ClassTag](
    f: (Iterator[RegionValue]) => Iterator[T]
  ): RDD[T] = crdd.mapPartitions(f).clearingRun

  def mapPartitions[T: ClassTag](
    f: (RVDContext, Iterator[RegionValue]) => Iterator[T]
  ): RDD[T] = crdd.cmapPartitions(f).clearingRun

  def mapPartitions(
    newTyp: RVDType
  )(f: (Iterator[RegionValue]) => Iterator[RegionValue]
  ): RVD = {
    require(newTyp.kType isPrefixOf typ.kType)
    RVD(
      newTyp,
      partitioner.coarsen(newTyp.key.length),
      crdd.mapPartitions(f))
  }

  def mapPartitions(
    newTyp: RVDType,
    f: (RVDContext, Iterator[RegionValue]) => Iterator[RegionValue]
  ): RVD = {
    require(newTyp.kType isPrefixOf typ.kType)
    RVD(
      newTyp,
      partitioner.coarsen(newTyp.key.length),
      crdd.cmapPartitions(f))
  }

  def mapPartitionsWithIndex[T: ClassTag](
    f: (Int, Iterator[RegionValue]) => Iterator[T]
  ): RDD[T] =  crdd.mapPartitionsWithIndex(f).clearingRun

  def mapPartitionsWithIndex[T: ClassTag](
    f: (Int, RVDContext, Iterator[RegionValue]) => Iterator[T]
  ): RDD[T] = crdd.cmapPartitionsWithIndex(f).clearingRun

  def mapPartitionsWithIndex(
    newTyp: RVDType
  )(f: (Int, Iterator[RegionValue]) => Iterator[RegionValue]
  ): RVD = {
    require(newTyp.kType isPrefixOf typ.kType)
    RVD(
      newTyp,
      partitioner.coarsen(newTyp.key.length),
      crdd.mapPartitionsWithIndex(f))
  }

  def mapPartitionsWithIndex(
    newTyp: RVDType,
    f: (Int, RVDContext, Iterator[RegionValue]) => Iterator[RegionValue]
  ): RVD = {
    require(newTyp.kType isPrefixOf typ.kType)
    RVD(
      newTyp,
      partitioner.coarsen(newTyp.key.length),
      crdd.cmapPartitionsWithIndex(f))
  }

  def zipWithIndex(name: String, partitionCounts: Option[IndexedSeq[Long]] = None): RVD = {
    assert(!typ.key.contains(name))

    val (newRowPType, ins) = rowType.physicalType.unsafeStructInsert(PInt64(), List(name))
    val newRowType = newRowPType.virtualType

    val a = sparkContext.broadcast(partitionCounts.map(_.toArray).getOrElse(countPerPartition()).scanLeft(0L)(_ + _))

    val newCRDD = crdd.cmapPartitionsWithIndex({ (i, ctx, it) =>
      val rv2 = RegionValue()
      val rvb = ctx.rvb
      var index = a.value(i)
      it.map { rv =>
        rvb.start(newRowType)
        ins(rv.region, rv.offset, rvb, () => rvb.addLong(index))
        index += 1
        rv2.set(rvb.region, rvb.end())
        rv2
      }
    }, preservesPartitioning=true)

    RVD(
      typ.copy(rowType = newRowType.asInstanceOf[TStruct]),
      partitioner,
      crdd = newCRDD
    )
  }

  // Filtering

  def head(n: Long, partitionCounts: Option[IndexedSeq[Long]]): RVD = {
    require(n >= 0)

    if (n == 0)
      return RVD.empty(sparkContext, typ)

    val newRDD = crdd.head(n, partitionCounts)
    val newNParts = newRDD.getNumPartitions
    assert(newNParts >= 0)

    val newRangeBounds = Array.range(0, newNParts).map(partitioner.rangeBounds)
    val newPartitioner = partitioner.copy(rangeBounds = newRangeBounds)

    RVD(typ, newPartitioner, newRDD)
  }

  def filter(p: (RegionValue) => Boolean): RVD =
    RVD(typ, partitioner, crddBoundary.filter(p))

  def filterWithContext[C](makeContext: (Int, RVDContext) => C, f: (C, RegionValue) => Boolean): RVD = {
    mapPartitionsWithIndex(typ, { (i, context, it) =>
      val c = makeContext(i, context)
      it.filter { rv =>
        if (f(c, rv))
          true
        else {
          rv.region.clear()
          false
        }
      }
    })
  }

  def filterIntervals(intervals: IntervalTree[_], keep: Boolean): RVD = {
    if (keep)
      filterToIntervals(intervals)
    else
      filterOutIntervals(intervals)
  }

  def filterOutIntervals(intervals: IntervalTree[_]): RVD = {
    val intervalsBc = crdd.sparkContext.broadcast(intervals)
    val kType = typ.kType
    val kPType = kType.physicalType
    val kRowFieldIdx = typ.kFieldIdx
    val rowType = typ.rowType

    mapPartitions(typ, { (ctx, it) =>
      val kUR = new UnsafeRow(kPType)
      it.filter { rv =>
        ctx.rvb.start(kType)
        ctx.rvb.selectRegionValue(rowType, kRowFieldIdx, rv)
        kUR.set(ctx.region, ctx.rvb.end())
        !intervalsBc.value.contains(kType.ordering, kUR)
      }
    })
  }

  def filterToIntervals(intervals: IntervalTree[_]): RVD = {
    val kOrdering = typ.kType.ordering
    val intervalsBc = crdd.sparkContext.broadcast(intervals)
    val rowType = typ.rowType.physicalType
    val kRowFieldIdx = typ.kFieldIdx

    val pred: (RegionValue) => Boolean = (rv: RegionValue) => {
      val ur = new UnsafeRow(rowType, rv)
      val key = Row.fromSeq(
        kRowFieldIdx.map(i => ur.get(i)))
      intervalsBc.value.contains(kOrdering, key)
    }

    val nPartitions = getNumPartitions
    if (nPartitions <= 1)
      return filter(pred)

    val newPartitionIndices = intervals.toIterator.flatMap { case (i, _) =>
      partitioner.getPartitionRange(i)
    }
      .toSet[Int] // distinct
      .toArray
      .sorted

    info(s"interval filter loaded ${ newPartitionIndices.length } of $nPartitions partitions")

    if (newPartitionIndices.isEmpty)
      RVD.empty(sparkContext, typ)
    else {
      subsetPartitions(newPartitionIndices).filter(pred)
    }
  }

  def subsetPartitions(keep: Array[Int]): RVD = {
    require(keep.length <= crdd.partitions.length, "tried to subset to more partitions than exist")
    require(keep.isIncreasing && (keep.isEmpty || (keep.head >= 0 && keep.last < crdd.partitions.length)),
      "values not increasing or not in range [0, number of partitions)")

    val newPartitioner = partitioner.copy(rangeBounds = keep.map(partitioner.rangeBounds))

    RVD(typ, newPartitioner, crdd.subsetPartitions(keep))
  }

  // Aggregating

  def aggregate[U: ClassTag](
    zeroValue: U
  )(seqOp: (U, RegionValue) => U,
    combOp: (U, U) => U
  ): U = {
    val clearingSeqOp = { (ctx: RVDContext, u: U, rv: RegionValue) =>
      val u2 = seqOp(u, rv)
      ctx.region.clear()
      u2
    }
    crdd.aggregate(zeroValue, clearingSeqOp, combOp)
  }

  def aggregateWithPartitionOp[PC, U: ClassTag](
    zeroValue: U,
    makePC: (Int, RVDContext) => PC
  )(seqOp: (PC, U, RegionValue) => Unit,
    combOp: (U, U) => U
  ): U = {
    crdd.cmapPartitionsWithIndex[U] { (i, ctx, it) =>
      val pc = makePC(i, ctx)
      val comb = zeroValue
      it.foreach { rv =>
        seqOp(pc, comb, rv)
        ctx.region.clear()
      }
      Iterator.single(comb)
    }.fold(zeroValue, combOp)
  }

  def treeAggregate[U: ClassTag](
    zeroValue: U
  )(seqOp: (U, RegionValue) => U,
    combOp: (U, U) => U,
    depth: Int = treeAggDepth(HailContext.get, crdd.getNumPartitions)
  ): U = {
    val clearingSeqOp = { (ctx: RVDContext, u: U, rv: RegionValue) =>
      val u2 = seqOp(u, rv)
      ctx.region.clear()
      u2
    }
    crdd.treeAggregate(zeroValue, clearingSeqOp, combOp, depth)
  }

  def treeAggregateWithPartitionOp[PC, U: ClassTag](
    zeroValue: U,
    makePC: (Int, RVDContext) => PC
  )(seqOp: (PC, U, RegionValue) => U,
    combOp: (U, U) => U,
    depth: Int = treeAggDepth(HailContext.get, crdd.getNumPartitions)
  ): U = {
    val clearingSeqOp = { (ctx: RVDContext, pc: PC, u: U, rv: RegionValue) =>
      val u2 = seqOp(pc, u, rv)
      ctx.region.clear()
      u2
    }
    crdd.treeAggregateWithPartitionOp(zeroValue, makePC, clearingSeqOp, combOp, depth)
  }

  def forall(p: RegionValue => Boolean): Boolean =
    crdd.map(p).clearingRun.forall(x => x)

  def exists(p: RegionValue => Boolean): Boolean =
    crdd.map(p).clearingRun.exists(x => x)

  def count(): Long =
    crdd.cmapPartitions { (ctx, it) =>
      var count = 0L
      it.foreach { rv =>
        count += 1
        ctx.region.clear()
      }
      Iterator.single(count)
    }.run.fold(0L)(_ + _)

  def countPerPartition(): Array[Long] =
    crdd.cmapPartitions { (ctx, it) =>
      var count = 0L
      it.foreach { rv =>
        count += 1
        ctx.region.clear()
      }
      Iterator.single(count)
    }.collect()

  def find(codec: CodecSpec, p: (RegionValue) => Boolean): Option[Array[Byte]] =
    filter(p).head(1, None).collectAsBytes(codec).headOption

  def find(region: Region)(p: (RegionValue) => Boolean): Option[RegionValue] =
    find(RVD.wireCodec, p).map(
      RegionValue.fromBytes(
        RVD.wireCodec.buildDecoder(rowType, rowType),
        region,
        RegionValue(region)))

  // Collecting

  def collect(codec: CodecSpec): Array[Row] = {
    val dec = codec.buildDecoder(rowType, rowType)
    val encodedData = collectAsBytes(codec)
    Region.scoped { region =>
      encodedData.iterator
        .map(RegionValue.fromBytes(dec, region, RegionValue(region)))
        .map { rv =>
          val row = SafeRow(rowType.physicalType, rv)
          region.clear()
          row
        }.toArray
    }
  }

  def collectPerPartition[T : ClassTag](f: (Int, RVDContext, Iterator[RegionValue]) => T): Array[T] =
    crdd.cmapPartitionsWithIndex { (i, ctx, it) =>
      Iterator.single(f(i, ctx, it))
    }.collect()

  def collectAsBytes(codec: CodecSpec): Array[Array[Byte]] = stabilize(codec).collect()

  // Persisting

  def cache(): RVD = persist(StorageLevel.MEMORY_ONLY)

  def persist(level: StorageLevel): RVD = {
    val localRowType = rowType
    val makeDec = RVD.memoryCodec.buildDecoder(localRowType, localRowType)

    val persistedRDD = stabilize().persist(level)

    val iterationRDD = destabilize(persistedRDD)

    new RVD(typ, partitioner, iterationRDD) {
      override def storageLevel: StorageLevel = persistedRDD.getStorageLevel

      override def persist(newLevel: StorageLevel): RVD = {
        if (newLevel == StorageLevel.NONE)
          unpersist()
        else {
          persistedRDD.persist(newLevel)
          this
        }
      }

      override def unpersist(): RVD = {
        persistedRDD.unpersist()
        self
      }
    }
  }

  def unpersist(): RVD = this

  def storageLevel: StorageLevel = StorageLevel.NONE

  def write(path: String, stageLocally: Boolean, codecSpec: CodecSpec): Array[Long] = {
    val (partFiles, partitionCounts) = crdd.writeRows(path, rowType, stageLocally, codecSpec)
    rvdSpec(codecSpec, partFiles).write(sparkContext.hadoopConfiguration, path)
    partitionCounts
  }

  def writeRowsSplit(
    path: String,
    t: MatrixType,
    codecSpec: CodecSpec,
    stageLocally: Boolean
  ): Array[Long] = crdd.writeRowsSplit(path, t, codecSpec, partitioner, stageLocally)

  // Joining

  def orderedLeftJoinDistinctAndInsert(
    right: RVD,
    root: String,
    lift: Option[String] = None,
    dropLeft: Option[Array[String]] = None
  ): RVD = {
    assert(!typ.key.contains(root))

    val valueStruct = right.typ.valueType
    val rightRowType = right.typ.rowType
    val liftField = lift.map { name => rightRowType.field(name) }
    val valueType = liftField.map(-_.typ).getOrElse(valueStruct)

    val removeLeft = dropLeft.map(_.toSet).getOrElse(Set.empty)
    val keepIndices = rowType.fields.filter(f => !removeLeft.contains(f.name)).map(_.index).toArray
    val newRowType = TStruct(
      keepIndices.map(i => rowType.fieldNames(i) -> rowType.types(i)) ++ Array(root -> valueType): _*)

    val localRowType = rowType

    val shouldLift = lift.isDefined
    val rightValueIndices = right.typ.valueFieldIdx
    val liftIndex = liftField.map(_.index).getOrElse(-1)

    val joiner = { (ctx: RVDContext, it: Iterator[JoinedRegionValue]) =>
      val rvb = ctx.rvb
      val rv = RegionValue()

      it.map { jrv =>
        val lrv = jrv.rvLeft
        val rrv = jrv.rvRight
        rvb.start(newRowType)
        rvb.startStruct()
        rvb.addFields(localRowType, lrv, keepIndices)
        if (rrv == null)
          rvb.setMissing()
        else {
          if (shouldLift)
            rvb.addField(rightRowType, rrv, liftIndex)
          else {
            rvb.startStruct()
            rvb.addFields(rightRowType, rrv, rightValueIndices)
            rvb.endStruct()
          }
        }
        rvb.endStruct()
        rv.set(ctx.region, rvb.end())
        rv
      }
    }
    assert(typ.key.length >= right.typ.key.length, s"$typ >= ${ right.typ }\n  $this\n  $right")
    orderedJoinDistinct(
      right,
      right.typ.key.length,
      "left",
      joiner,
      typ.copy(rowType = newRowType,
        key = typ.key.filter(!removeLeft.contains(_)))
    )
  }

  def orderedJoin(
    right: RVD,
    joinType: String,
    joiner: (RVDContext, Iterator[JoinedRegionValue]) => Iterator[RegionValue],
    joinedType: RVDType
  ): RVD =
    orderedJoin(right, typ.key.length, joinType, joiner, joinedType)

  def orderedJoin(
    right: RVD,
    joinKey: Int,
    joinType: String,
    joiner: (RVDContext, Iterator[JoinedRegionValue]) => Iterator[RegionValue],
    joinedType: RVDType
  ): RVD =
    keyBy(joinKey).orderedJoin(right.keyBy(joinKey), joinType, joiner, joinedType)

  def orderedJoinDistinct(
    right: RVD,
    joinType: String,
    joiner: (RVDContext, Iterator[JoinedRegionValue]) => Iterator[RegionValue],
    joinedType: RVDType
  ): RVD =
    orderedJoinDistinct(right, typ.key.length, joinType, joiner, joinedType)

  def orderedJoinDistinct(
    right: RVD,
    joinKey: Int,
    joinType: String,
    joiner: (RVDContext, Iterator[JoinedRegionValue]) => Iterator[RegionValue],
    joinedType: RVDType
  ): RVD =
    keyBy(joinKey).orderedJoinDistinct(right.keyBy(joinKey), joinType, joiner, joinedType)

  def orderedZipJoin(right: RVD): ContextRDD[RVDContext, JoinedRegionValue] =
    orderedZipJoin(right, typ.key.length)

  def orderedZipJoin(right: RVD, joinKey: Int): ContextRDD[RVDContext, JoinedRegionValue] =
    keyBy(joinKey).orderedZipJoin(right.keyBy(joinKey))

  def orderedMerge(right: RVD): RVD =
    orderedMerge(right, typ.key.length)

  def orderedMerge(right: RVD, joinKey: Int): RVD =
    keyBy(joinKey).orderedMerge(right.keyBy(joinKey))

  def partitionSortedUnion(rdd2: RVD): RVD = {
    assert(typ == rdd2.typ)
    assert(partitioner == rdd2.partitioner)

    val localTyp = typ
    zipPartitions(typ, rdd2) { (ctx, it, it2) =>
      new Iterator[RegionValue] {
        private val bit = it.buffered
        private val bit2 = it2.buffered
        private val rv = RegionValue()

        def hasNext: Boolean = bit.hasNext || bit2.hasNext

        def next(): RegionValue = {
          val old =
            if (!bit.hasNext)
              bit2.next()
            else if (!bit2.hasNext)
              bit.next()
            else {
              val c = localTyp.kInRowOrd.compare(bit.head, bit2.head)
              if (c < 0)
                bit.next()
              else
                bit2.next()
            }
          ctx.rvb.start(localTyp.rowType)
          ctx.rvb.addRegionValue(localTyp.rowType, old)
          rv.set(ctx.region, ctx.rvb.end())
          rv
        }
      }
    }
  }

  // Zipping

  def zip(
    newTyp: RVDType,
    that: RVD
  )(zipper: (RVDContext, RegionValue, RegionValue) => RegionValue
  ): RVD = RVD(
    newTyp,
    partitioner,
    boundary.crdd.czip(that.boundary.crdd)(zipper))

  def zipPartitions(
    newTyp: RVDType,
    that: RVD
  )(zipper: (RVDContext, Iterator[RegionValue], Iterator[RegionValue]) => Iterator[RegionValue]
  ): RVD = RVD(
    newTyp,
    partitioner,
    boundary.crdd.czipPartitions(that.boundary.crdd)(zipper))

  def zipPartitions[T: ClassTag](
    newTyp: RVDType,
    that: ContextRDD[RVDContext, T]
  )(zipper: (Iterator[RegionValue], Iterator[T]) => Iterator[RegionValue]
  ): RVD = RVD(
    newTyp,
    partitioner,
    boundary.crdd.zipPartitions(that)(zipper))

  def zipPartitionsAndContext(
    newTyp: RVDType,
    that: RVD
  )(zipper: (RVDContext, RVDContext => Iterator[RegionValue], RVDContext => Iterator[RegionValue]) => Iterator[RegionValue]
  ): RVD = RVD(
    newTyp,
    partitioner,
    crdd.czipPartitionsAndContext(that.crdd) { (ctx, lit, rit) =>
      zipper(ctx, ctx => lit.flatMap(_ (ctx)), ctx => rit.flatMap(_ (ctx)))
    }
  )

  def zipPartitionsWithIndex(
    newTyp: RVDType,
    that: RVD
  )(zipper: (Int, RVDContext, Iterator[RegionValue], Iterator[RegionValue]) => Iterator[RegionValue]
  ): RVD = RVD(
    newTyp,
    partitioner,
    boundary.crdd.czipPartitionsWithIndex(that.boundary.crdd)(zipper))

  // New key type must be prefix of left key type. 'joinKey' must be prefix of
  // both left key and right key. 'zipper' must take all output key values from
  // left iterator, and be monotonic on left iterator (it can drop or duplicate
  // elements of left iterator, or insert new elements in order, but cannot
  // rearrange them), and output region values must conform to 'newTyp'. The
  // partitioner of the resulting RVD will be left partitioner truncated
  // to new key. Each partition will be computed by 'zipper', with corresponding
  // partition of 'this' as first iterator, and with all rows of 'that' whose
  // 'joinKey' might match something in partition as the second iterator.
  def alignAndZipPartitions(
    newTyp: RVDType,
    that: RVD,
    joinKey: Int
  )(zipper: (RVDContext, Iterator[RegionValue], Iterator[RegionValue]) => Iterator[RegionValue]
  ): RVD = {
    require(newTyp.kType isPrefixOf this.typ.kType)
    require(joinKey <= this.typ.key.length)
    require(joinKey <= that.typ.key.length)

    val left = this.truncateKey(newTyp.key)
    RVD(
      typ = newTyp,
      partitioner = left.partitioner,
      crdd = left.crddBoundary.czipPartitions(
        RepartitionedOrderedRDD2(that, this.partitioner.coarsenedRangeBounds(joinKey)).boundary
      )(zipper))
  }

  // Private

  private[rvd] def copy(
    typ: RVDType = typ,
    partitioner: RVDPartitioner = partitioner,
    crdd: ContextRDD[RVDContext, RegionValue] = crdd
  ): RVD =
    RVD(typ, partitioner, crdd)

  private[rvd] def destabilize(
    stable: RDD[Array[Byte]],
    codec: CodecSpec = RVD.memoryCodec
  ): ContextRDD[RVDContext, RegionValue] = {
    val dec = codec.buildDecoder(rowType, rowType)
    ContextRDD.weaken[RVDContext](stable).cmapPartitions { (ctx, it) =>
      val rv = RegionValue(ctx.region)
      it.map(RegionValue.fromBytes(dec, ctx.region, rv))
    }
  }

  private[rvd] def stably(
    f: RDD[Array[Byte]] => RDD[Array[Byte]]
  ): ContextRDD[RVDContext, RegionValue] = destabilize(f(stabilize()))

  private[rvd] def crddBoundary: ContextRDD[RVDContext, RegionValue] =
    crdd.boundary

  private[rvd] def keyBy(key: Int = typ.key.length): KeyedRVD =
    new KeyedRVD(this, key)

  private def rvdSpec(codecSpec: CodecSpec, partFiles: Array[String]): RVDSpec =
    OrderedRVDSpec(
      typ,
      codecSpec,
      partFiles,
      JSONAnnotationImpex.exportAnnotation(
        partitioner.rangeBounds,
        partitioner.rangeBoundsType))
}

object RVD {
  val memoryCodec = CodecSpec.defaultUncompressed

  val wireCodec = memoryCodec

  def empty(sc: SparkContext, typ: RVDType): RVD = {
    RVD(typ,
      RVDPartitioner.empty(typ),
      ContextRDD.empty[RVDContext, RegionValue](sc))
  }

  def unkeyed(rowType: TStruct, crdd: ContextRDD[RVDContext, RegionValue]): RVD =
    new RVD(
      RVDType(rowType),
      RVDPartitioner.unkeyed(crdd.getNumPartitions),
      crdd)

  def getKeys(
    typ: RVDType,
    crdd: ContextRDD[RVDContext, RegionValue]
  ): ContextRDD[RVDContext, RegionValue] = {
    // The region values in 'crdd' are of type `typ.rowType`
    val localType = typ
    crdd.cmapPartitions { (ctx, it) =>
      val wrv = WritableRegionValue(localType.kType, ctx.freshRegion)
      it.map { rv =>
        wrv.setSelect(localType.rowType, localType.kFieldIdx, rv)
        wrv.value
      }
    }
  }

  def getKeyInfo(
    typ: RVDType,
    // 'partitionKey' is used to check whether the rows are ordered by the first
    // 'partitionKey' key fields, even if they aren't ordered by the full key.
    partitionKey: Int,
    keys: ContextRDD[RVDContext, RegionValue]
  ): Array[RVDPartitionInfo] = {
    // the region values in 'keys' are of typ `typ.keyType`
    val nPartitions = keys.getNumPartitions
    if (nPartitions == 0)
      return Array()

    val rng = new java.util.Random(1)
    val partitionSeed = Array.fill[Int](nPartitions)(rng.nextInt())

    val sampleSize = math.min(nPartitions * 20, 1000000)
    val samplesPerPartition = sampleSize / nPartitions

    val localType = typ

    val keyInfo = keys.cmapPartitionsWithIndex { (i, ctx, it) =>
      val out = if (it.hasNext)
        Iterator(RVDPartitionInfo(localType, partitionKey, samplesPerPartition, i, it, partitionSeed(i), ctx))
      else
        Iterator()
      out
    }.collect()

    keyInfo.sortBy(_.min)(typ.kType.ordering.toOrdering)
  }

  def coerce(
    typ: RVDType,
    crdd: ContextRDD[RVDContext, RegionValue]
  ): RVD = coerce(typ, typ.key.length, crdd)

  def coerce(
    typ: RVDType,
    crdd: ContextRDD[RVDContext, RegionValue],
    fastKeys: ContextRDD[RVDContext, RegionValue]
  ): RVD = coerce(typ, typ.key.length, crdd, fastKeys)

  def coerce(
    typ: RVDType,
    partitionKey: Int,
    crdd: ContextRDD[RVDContext, RegionValue]
  ): RVD = {
    val keys = getKeys(typ, crdd)
    makeCoercer(typ, partitionKey, keys).coerce(typ, crdd)
  }

  def coerce(
    typ: RVDType,
    partitionKey: Int,
    crdd: ContextRDD[RVDContext, RegionValue],
    keys: ContextRDD[RVDContext, RegionValue]
  ): RVD = {
    val keys = getKeys(typ, crdd)
    makeCoercer(typ, partitionKey, keys).coerce(typ, crdd)
  }

  def makeCoercer(
    fullType: RVDType,
    // keys: RDD[RegionValue[fullType.kType]]
    keys: ContextRDD[RVDContext, RegionValue]
  ): RVDCoercer = makeCoercer(fullType, fullType.key.length, keys)

  def makeCoercer(
    fullType: RVDType,
    partitionKey: Int,
    // keys: RDD[RegionValue[fullType.kType]]
    keys: ContextRDD[RVDContext, RegionValue]
   ): RVDCoercer = {
    type CRDD = ContextRDD[RVDContext, RegionValue]
    val sc = keys.sparkContext

    val unkeyedCoercer: RVDCoercer = new RVDCoercer(fullType) {
      def _coerce(typ: RVDType, crdd: CRDD): RVD = {
        assert(typ.key.isEmpty)
        unkeyed(typ.rowType, crdd)
      }
    }

    if (fullType.key.isEmpty)
      return unkeyedCoercer

    val emptyCoercer: RVDCoercer = new RVDCoercer(fullType) {
      def _coerce(typ: RVDType, crdd: CRDD): RVD = empty(sc, typ)
    }

    val keyInfo = getKeyInfo(fullType, partitionKey, keys)

    if (keyInfo.isEmpty)
      return emptyCoercer

    val bounds = keyInfo.map(_.interval).toFastIndexedSeq
    val pkBounds = bounds.map(_.coarsen(partitionKey))
    def orderPartitions = { crdd: CRDD =>
      val pids = keyInfo.map(_.partitionIndex)
      if (pids.isSorted && crdd.getNumPartitions == pids.length) {
        assert(pids.isEmpty || pids.last < crdd.getNumPartitions)
        crdd
      }
      else {
        assert(pids.isEmpty || pids.max < crdd.getNumPartitions)
        if (!pids.isSorted)
          info("Coerced dataset with out-of-order partitions.")
        crdd.reorderPartitions(pids)
      }
    }
    val intraPartitionSortedness = keyInfo.map(_.sortedness).min

    if (intraPartitionSortedness == RVDPartitionInfo.KSORTED
        && RVDPartitioner.isValid(fullType.kType, bounds)) {

      info("Coerced sorted dataset")

      new RVDCoercer(fullType) {
        val unfixedPartitioner =
          new RVDPartitioner(fullType.kType, bounds)
        val newPartitioner = RVDPartitioner.generate(
          fullType.key.take(partitionKey), fullType.kType, bounds)

        def _coerce(typ: RVDType, crdd: CRDD): RVD = {
          RVD(typ, unfixedPartitioner, orderPartitions(crdd))
            .repartition(newPartitioner, shuffle = false)
        }
      }

    } else if (intraPartitionSortedness >= RVDPartitionInfo.TSORTED
        && RVDPartitioner.isValid(fullType.kType.truncate(partitionKey), pkBounds)) {

      info("Coerced almost-sorted dataset")

      new RVDCoercer(fullType) {
        val unfixedPartitioner = new RVDPartitioner(
          fullType.kType.truncate(partitionKey),
          pkBounds
        )
        val newPartitioner = RVDPartitioner.generate(
          fullType.key.take(partitionKey),
          fullType.kType.truncate(partitionKey),
          pkBounds
        )

        def _coerce(typ: RVDType, crdd: CRDD): RVD = {
          RVD(
            typ.copy(key = typ.key.take(partitionKey)),
            unfixedPartitioner,
            orderPartitions(crdd)
          ) .repartition(newPartitioner, shuffle = false)
            .localSort(typ.key)
        }
      }

    } else {

      info("Ordering unsorted dataset with network shuffle")

      new RVDCoercer(fullType) {
        val newPartitioner =
          calculateKeyRanges(fullType, keyInfo, keys.getNumPartitions, partitionKey)

        def _coerce(typ: RVDType, crdd: CRDD): RVD = {
          RVD.unkeyed(typ.rowType, crdd)
            .repartition(newPartitioner, shuffle = true)
        }
      }
    }
  }

  def calculateKeyRanges(
    typ: RVDType,
    pInfo: Array[RVDPartitionInfo],
    nPartitions: Int,
    partitionKey: Int
  ): RVDPartitioner = {
    assert(nPartitions > 0)
    assert(pInfo.nonEmpty)

    val kord = typ.kType.ordering.toOrdering
    val min = pInfo.map(_.min).min(kord)
    val max = pInfo.map(_.max).max(kord)
    val samples = pInfo.flatMap(_.samples)

    RVDPartitioner.fromKeySamples(typ, min, max, samples, nPartitions, partitionKey)
  }

  def apply(
    typ: RVDType,
    partitioner: RVDPartitioner,
    codec: CodecSpec,
    rdd: RDD[Array[Byte]]
  ): RVD = {
    val dec = codec.buildDecoder(typ.rowType, typ.rowType)
    apply(
      typ,
      partitioner,
      ContextRDD.weaken[RVDContext](rdd).cmapPartitions { (ctx, it) =>
        val rv = RegionValue()
        it.map(RegionValue.fromBytes(dec, ctx.region, rv))
      })
  }

  def alignAndZipNPartitions(
    orvds: IndexedSeq[OrderedRVD],
    newTyp: OrderedRVDType
  )(zipper: (RVDContext, Array[Iterator[RegionValue]]) => Iterator[RegionValue]
  ): OrderedRVD = {
    val first = orvds.head
    require(newTyp.kType isIsomorphicTo first.typ.kType)
    require(orvds.forall(_.typ.kType isIsomorphicTo first.typ.kType))

    val crdds = orvds.map(RepartitionedOrderedRDD2(_, first.partitioner.rangeBounds).boundary)

    OrderedRVD(
      typ = newTyp,
      partitioner = first.partitioner,
      crdd = ContextRDD.czipNPartitions(crdds)(zipper))
  }

  def apply(
    typ: RVDType,
    partitioner: RVDPartitioner,
    crdd: ContextRDD[RVDContext, RegionValue]
  ): RVD = {
    val sc = crdd.sparkContext

    val partitionerBc = partitioner.broadcast(sc)
    val localType = typ
    val localKPType = typ.kType.physicalType

    new RVD(
      typ,
      partitioner,
      crdd.cmapPartitionsWithIndex { case (i, ctx, it) =>
        val prevK = WritableRegionValue(localType.kType, ctx.freshRegion)
        val kUR = new UnsafeRow(localKPType)

        new Iterator[RegionValue] {
          var first = true

          def hasNext: Boolean = it.hasNext

          def next(): RegionValue = {
            val rv = it.next()

            if (first)
              first = false
            else {
              if (localType.kRowOrd.gt(prevK.value, rv)) {
                kUR.set(prevK.value)
                val prevKeyString = kUR.toString()

                prevK.setSelect(localType.rowType, localType.kFieldIdx, rv)
                kUR.set(prevK.value)
                val currKeyString = kUR.toString()
                fatal(
                  s"""RVD error! Keys found out of order:
                     |  Current key:  $currKeyString
                     |  Previous key: $prevKeyString
                     |This error can occur after a split_multi if the dataset
                     |contains both multiallelic variants and duplicated loci.
                   """.stripMargin)
              }
            }

            prevK.setSelect(localType.rowType, localType.kFieldIdx, rv)
            kUR.set(prevK.value)

            if (!partitionerBc.value.rangeBounds(i).contains(localType.kType.ordering, kUR))
              fatal(
                s"""RVD error! Unexpected key in partition $i
                   |  Range bounds for partition $i: ${ partitionerBc.value.rangeBounds(i) }
                   |  Range of partition IDs for key: [${ partitionerBc.value.getSafePartitionLowerBound(kUR) }, ${ partitionerBc.value.getSafePartitionUpperBound(kUR) })
                   |  Invalid key: ${ kUR.toString() }""".stripMargin)

            rv
          }
        }
      })
  }

  def union(rvds: Seq[RVD], joinKey: Int): RVD = rvds match {
    case Seq(x) => x
    case first +: _ =>
      if (joinKey == 0) {
        val sc = first.sparkContext
        RVD.unkeyed(first.rowType, ContextRDD.union(sc, rvds.map(_.crdd)))
      } else
        rvds.reduce(_.orderedMerge(_, joinKey))
  }

  def union(rvds: Seq[RVD]): RVD =
    union(rvds, rvds.head.typ.key.length)
}
