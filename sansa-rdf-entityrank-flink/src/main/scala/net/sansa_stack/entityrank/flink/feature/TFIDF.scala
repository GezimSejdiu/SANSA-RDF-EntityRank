package net.sansa_stack.entityrank.flink.feature

import org.apache.flink.api.scala._
import breeze.linalg
import org.apache.flink.ml.common.{ Parameter, ParameterMap }
import org.apache.flink.ml.math.{ BreezeVectorConverter, Vector }
import org.apache.flink.ml.pipeline.{
  TransformOperation,
  FitOperation,
  Transformer
}

class TFIDF extends Transformer[TFIDF] {

  private[feature] var vectorsOption: Option[DataSet[linalg.Vector[Double]]] = None

}

object TFIDF {

  def apply(): TFIDF = {
    new TFIDF()
  }

  implicit def fit[T <: Vector] = new FitOperation[TFIDF, T] {
    override def fit(instance: TFIDF, fitParameters: ParameterMap, input: DataSet[T]): Unit = {
      val vectors = transform(input)
      instance.vectorsOption = Some(vectors)
    }
  }

  private def transform[T <: Vector](dataSet: DataSet[T]): DataSet[linalg.Vector[Double]] = {
    val transIDF = dataSet.map(f => linalg.Vector.zeros[Double](1))
    transIDF
  }

}
