package net.sansa_stack.entityrank.flink.feature

import org.apache.flink.ml.pipeline.{
  TransformOperation,
  FitOperation,
  Transformer
}
import org.apache.flink.ml.common.{ LabeledVector, Parameter,ParameterMap }
import org.apache.flink.ml.pipeline.{TransformDataSetOperation, FitOperation,Transformer}
import org.apache.flink.api.scala._
import breeze.linalg
import net.sansa_stack.entityrank.flink.feature.HashingTermFrequency.{ Binary, HashAlgorithm }
import org.apache.flink.api.common.typeinfo.TypeInformation
import scala.reflect.ClassTag
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{BreezeVectorConverter, Vector}

/**
 * Maps a sequence of terms to their term frequencies using the hashing trick.
 */
class HashingTermFrequency(val numFeatures: Int) extends Transformer[HashingTermFrequency] {

  private[feature] var vectorsOption: Option[DataSet[linalg.Vector[Double]]] = None
  
   private [feature] var metricsOption: Option[
      DataSet[(linalg.Vector[Double], linalg.Vector[Double])]
    ] = None

  def this() = this(1 << 20)

  /**
   * Sets the binary value to the term counts.
   *  If true, term frequency vector will be binary such that non-zero term counts will be set to 1
   *  (default:false)
   *
   * @param value the user-specified  value.
   * @return the HashingTermFrequency instance with its Binary value set to the user-specified value
   */
  def setBinary(value: Boolean): HashingTermFrequency = {
    parameters.add(Binary, value)
    this
  }

  def setHashAlgorithm(value: String): HashingTermFrequency = {
    parameters.add(HashAlgorithm, value)
    this
  }

}

object HashingTermFrequency {

  // ====================================== Parameters =============================================

  case object Binary extends Parameter[Boolean] {
    override val defaultValue: Option[Boolean] = Some(false);
  }

  case object HashAlgorithm extends Parameter[String] {
    override val defaultValue: Option[String] = Some("native")
  }

// ==================================== Factory methods ==========================================

  def apply(): HashingTermFrequency = {
    new HashingTermFrequency(1 << 20)
  }
  
 // ====================================== Operations =============================================
  
implicit def transform[T <: Vector : BreezeVectorConverter : TypeInformation : ClassTag]
  = {
    new TransformDataSetOperation[HashingTermFrequency, T, T] {
      override def transformDataSet(
        instance: HashingTermFrequency,
        transformParameters: ParameterMap,
        input: DataSet[T])
      : DataSet[T] = {

        val resultingParameters = instance.parameters ++ transformParameters
        val binary = resultingParameters(Binary)
        val hashAlgorithm = resultingParameters(HashAlgorithm)

        instance.metricsOption match {
          case Some(metrics) => {
            input.mapWithBcVariable(metrics) {
              (vector, metrics) => {
                val (broadcastBinary, broadcastHashAlgorithm) = metrics
                transformVector(vector, broadcastBinary, broadcastHashAlgorithm)
              }
            }
          }

          case None =>
            throw new RuntimeException("The HashingTermFrequency has not been fitted to the data. ")
        }
      }
    }
}

private def transformVector[T <: Vector: BreezeVectorConverter](
      vector: T,
      broadcastBinary: linalg.Vector[Double],
      broadcastHashAlgorithm: linalg.Vector[Double]
      )
    : T = {
    var myVector = vector.asBreeze
    
    //TODO: transform data to hashing code

    myVector :/= broadcastHashAlgorithm
    myVector.fromBreeze
  }


  
}