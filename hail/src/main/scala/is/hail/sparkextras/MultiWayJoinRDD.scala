package is.hail.sparkextras

import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private class MultiWayJoinPartition[T: ClassTag](
  idx: Int,
  @transient private val rdds: Array[RDD[T]]
) extends Partition {
  override val index: Int = idx
  var partitionVals = rdds.map(rdd => rdd.partitions(idx))
  def partitions = partitionVals
}

class MultiWayJoinRDD[T: ClassTag, V: ClassTag](
  sc: SparkContext,
  var rdds: Array[RDD[T]],
  f: (Array[Iterator[T]]) => Iterator[V],
  preservesPartioning: Boolean = false
) extends RDD[V](sc, rdds.map(x => new OneToOneDependency(x))) {

    override def getPartitions: Array[Partition] = {
      val numParts = rdds(0).partitions.length
      require(rdds.forall(rdd => rdd.partitions.length == numParts))
      Array.tabulate[Partition](numParts)(new MultiWayJoinPartition(_, rdds))
    }

    override def compute(s: Partition, tc: TaskContext) = {
      val partitions = s.asInstanceOf[MultiWayJoinPartition[T]].partitions
      val arr = Array.tabulate(rdds.length)(i => rdds(i).iterator(partitions(i), tc))
      f(arr)
    }
}
