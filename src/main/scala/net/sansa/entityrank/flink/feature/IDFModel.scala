package net.sansa.entityrank.flink.feature

import org.apache.flink.api.scala.{ DataSet, ExecutionEnvironment }
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.functions.RichMapPartitionFunction

class IDF{

  /**
   * Computes the inverse document frequency.
   * @param dataset an DataSet of term frequency vectors
   */
  def fit(dataset: DataSet[Vector]): IDFModel = {
    new IDFModel(null)
  }
}
private object IDF {
}

  /**
   * Represents an IDF model that can transform term frequency vectors.
   */
  class IDFModel private[entityrank] (val idf: Vector) extends Serializable {

    /**
     * Transforms term frequency (TF) vectors to TF-IDF vectors.
     *
     * @param dataset an RDD of term frequency vectors
     * @return an RDD of TF-IDF vectors
     */
   /* def transform(dataset: DataSet[Vector]): DataSet[Vector] = {

      //  val bcIdf = getExecutionEnvironment..getBroadcastVariable[B]("broadcastVariable").get(0)

      //dataset.mapPartitions(iter => iter.map(v => IDFModel.transform(bcIdf.value, v)))

      val res = dataset.map(new RichMapPartitionFunction[DataSet[Vector]]() {

        var broadcastNumberOfWords: java.util.List[(Int)] = null

        override def open(config: Configuration): Unit = {
          broadcastNumberOfWords = getRuntimeContext().getBroadcastVariable[Int]("broadcastSetName")
        }

        def mapPartitions(dataset: DataSet[Vector]): DataSet[Vector] = {
          dataset.mapPartitions(iter => iter.map(v => IDFModel.transform(broadcastNumberOfWords.get(0), v)))
        }

      }).withBroadcastSet(numberOfWords, "broadcastSetName")
      res

    }
    */
    /* def transform(dataset: RDD[Vector]): RDD[Vector] = {
    val bcIdf = dataset.context.broadcast(idf)
    dataset.mapPartitions(iter => iter.map(v => IDFModel.transform(bcIdf.value, v)))
  }*/

    /**
     * Transforms a term frequency (TF) vector to a TF-IDF vector
     *
     * @param v a term frequency vector
     * @return a TF-IDF vector
     */
    def transform(v: Vector): Vector = IDFModel.transform(idf, v)
  }

  private object IDFModel {

    /**
     * Transforms a term frequency (TF) vector to a TF-IDF vector with a IDF vector
     *
     * @param idf an IDF vector
     * @param v a term frequency vector
     * @return a TF-IDF vector
     */
    def transform(idf: Vector, v: Vector): Vector = {
      val n = v.size
      v match {
        case SparseVector(size, indices, values) =>
          val nnz = indices.length
          val newValues = new Array[Double](nnz)
          var k = 0
          while (k < nnz) {
            newValues(k) = values(k) * idf(indices(k))
            k += 1
          }
          Vectors.sparse(n, indices, newValues)
        case DenseVector(values) =>
          val newValues = new Array[Double](n)
          var j = 0
          while (j < n) {
            newValues(j) = values(j) * idf(j)
            j += 1
          }
          Vectors.dense(newValues)
        case other =>
          throw new UnsupportedOperationException(
            s"Only sparse and dense vectors are supported but got ${other.getClass}.")
     }
  }
}