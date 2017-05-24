package per.harenp.Hedgehog

import org.apache.spark.Partitioner

/**
 * SymbolPartitioner
 *   - to keep processing of a symbol's file or url on the same partition
 *
 * @author patrick.haren@yahoo,com
 */
class SymbolPartitioner extends Partitioner
{
  val numParts = 500
  def actualSymbol(key: Any) = {
    val fullPath = key.toString
    val namePart = fullPath.split("/").last
    namePart.split(Array('-','.')).head
  }
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    val code = actualSymbol(key).hashCode % numPartitions
    if (code < 0)
      code + numPartitions
    else
      code
    }
  // Java equals method to let Spark compare our Partitioner objects
  override def equals(other: Any): Boolean = other match {
    case sp: SymbolPartitioner =>
      sp.numPartitions == numPartitions
    case _ =>
      false
  }
}