package net.sansa_stack.entityrank.flink.feature

import org.apache.flink.api.scala.{ DataSet, ExecutionEnvironment }
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.ml.math.DenseVector
import breeze.linalg.{ DenseVector => BDV }
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import scala.reflect.ClassTag
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.functions.RichMapPartitionFunction
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

class IDF {

  /**
   * Computes the inverse document frequency.
   * @param dataset an DataSet of term frequency vectors
   */
  def fit(dataset: DataSet[Vector]): IDFModel = {

    /*val idf = dataset.aggregate(new IDF.DocumentFrequencyAggregator,"")(
      seqOp = (df, v) => df.add(v),
      combOp = (df1, df2) => df1.merge(df2)).idf()
    new IDFModel(idf)*/
    new IDFModel(null)
  }

  /* def combineGroupWith[R: TypeInformation: ClassTag](fun: DataSet[Vector] => R): DataSet[R] =
    dataset.combineGroup {
      (it, out) =>
        out.collect(fun(it.toStream))
    }*/

}
private object IDF {
  /** Document frequency aggregator. */
  class DocumentFrequencyAggregator extends Serializable {

    /** number of documents */
    private var m = 0L
    /** document frequency vector */
    private var df: BDV[Long] = _

    /** Adds a new document. */
    def add(doc: Vector): this.type = {
      if (isEmpty) {
        df = BDV.zeros(doc.size)
      }
      doc match {
        case SparseVector(size, indices, values) =>
          val nnz = indices.size
          var k = 0
          while (k < nnz) {
            if (values(k) > 0) {
              df(indices(k)) += 1L
            }
            k += 1
          }
        case DenseVector(values) =>
          val n = values.size
          var j = 0
          while (j < n) {
            if (values(j) > 0.0) {
              df(j) += 1L
            }
            j += 1
          }
        case other =>
          throw new UnsupportedOperationException(
            s"Only sparse and dense vectors are supported but got ${other.getClass}.")
      }
      m += 1L
      this
    }

    /** Merges another. */
    def merge(other: DocumentFrequencyAggregator): this.type = {
      if (!other.isEmpty) {
        m += other.m
        if (df == null) {
          df = other.df.copy
        } else {
          df += other.df
        }
      }
      this
    }

    private def isEmpty: Boolean = m == 0L

    /** Returns the current IDF vector. */
    def idf(): Vector = {
      if (isEmpty) {
        throw new IllegalStateException("Haven't seen any document yet.")
      }
      val n = df.length
      val inv = new Array[Double](n)
      var j = 0
      while (j < n) {

        inv(j) = math.log((m + 1.0) / (df(j) + 1.0))
        j += 1
      }
      Vectors.dense(inv)
    }

    /*  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
                                                combOp: (U, U) => U): V=
                                                {
                                                 seqOp = (df, v) => df.add(v),
      combOp = (df1, df2) => df1.merge(df2)).idf()
                                                }*/

  }
}

/**
 * Represents an IDF model that can transform term frequency vectors.
 */
class IDFModel private[entityrank] (val idf: Vector) extends Serializable {

  /**
   * Transforms term frequency (TF) vectors to TF-IDF vectors.
   *
   * @param dataset an DataSet of term frequency vectors
   * @return an DataSet of TF-IDF vectors
   */
  def transform(dataset: DataSet[Vector]): DataSet[Vector] = {

    //dataset.map(new SelectNearestClusterCenter).withBroadcastSet(dataset, "bcIdf")//Broadcast the DataSet
    dataset.mapPartition(iter => iter.map(v => IDFModel.transform(idf, v)))
  }
  /* def transform(dataset: DataSet[Vector]): DataSet[Vector] = {
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

final class SelectNearestClusterCenter extends RichMapFunction[DataSet[Vector], DataSet[Vector]] {

  /** Key for bcIdf broadcasts. */
  val bcIdfBc = "bcIdf"

  private var bcIdf: java.util.List[Vector] = _

  /** Reads the bcIdf values from a broadcast variable into a collection. */
  override def open(parameters: Configuration) {
    super.open(parameters)
    //Access the broadcasted DataSet as a Collection
    bcIdf = getRuntimeContext().getBroadcastVariable[Vector](bcIdfBc)

  }

  override def map(dataset: DataSet[Vector]): DataSet[Vector] = {
    dataset.mapPartition(iter => iter.map(v => IDFModel.transform(bcIdf.get(0), v)))
  }

}

