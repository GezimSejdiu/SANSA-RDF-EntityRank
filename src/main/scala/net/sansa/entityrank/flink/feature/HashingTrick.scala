package net.sansa.entityrank.flink.feature
import org.apache.flink.ml.math
import scala.collection.mutable
import net.sansa.entityrank.flink.utils.Utils
import org.apache.flink.api.scala.DataSet
import scala.math.log
import scala.util.hashing.MurmurHash3
import scala.collection.mutable.LinkedHashSet
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.math.SparseVector
import org.apache.flink.api.common.typeinfo.TypeInformation
import scala.reflect.ClassTag

/**
 * Maps a sequence of terms to their term frequencies using the hashing trick.
 */

class HashingTrick(val numFeatures: Int) extends Serializable {

  import HashingTrick._

  private var binary = false
  private var hashAlgorithm = HashingTrick.Native

  def this() = this(1 << 20)

  /**
   * If true, term frequency vector will be binary such that non-zero term counts will be set to 1
   * (default: false)
   */
  def setBinary(value: Boolean): this.type = {
    binary = value
    this
  }

  /**
   * Set the hash algorithm used when mapping term to integer.
   * (default: native)
   */
  def setHashAlgorithm(value: String): this.type = {
    hashAlgorithm = value
    this
  }

  /**
   * Returns the index of the input term.
   */
  def indexOf(term: Any): Int = {
    Utils.nonNegativeMod(getHashFunction(term), numFeatures)
  }

  /**
   * Get the hash function corresponding to the current [[hashAlgorithm]] setting.
   */
  private def getHashFunction: Any => Int = hashAlgorithm match {
    case Native => nativeHash
    case _ =>
      throw new IllegalArgumentException(
        s"HashingTF does not recognize hash algorithm $hashAlgorithm")
  }

  /**
   * Transforms the input document into a sparse term frequency vector.
   */
  def transform(document: Iterable[_]): Vector = {
    val termFrequencies = mutable.HashMap.empty[Int, Double]
    val setTF = if (binary) (i: Int) => 1.0 else (i: Int) => termFrequencies.getOrElse(i, 0.0) + 1.0
    val hashFunc: Any => Int = getHashFunction
    document.foreach { term =>
      val i = Utils.nonNegativeMod(hashFunc(term), numFeatures)
      termFrequencies.put(i, setTF(i))
    }
    Vectors.sparse(numFeatures, termFrequencies.toSeq)
  }
  
  
  

  /**
   * Transforms the input document to term frequency vectors.
   */
  /*  def transform[D <: Iterable[_]](dataset: DataSet[D]): DataSet[Vector] = {
    dataset.map(this.transform)
    }
    
    */

  // def dd(dataset: DataSet[Vector]) = transform(dataset.collect.toIndexedSeq)
  /*  def transform(dataset: DataSet[Vector]): DataSet[Vector] = {
    dataset.map(x => 
      x match {
        case x:Vector => this.transform(x)
    })
  }*/

  /*  def transform[D: ClassTag: TypeInformation](dataset: DataSet[D]): DataSet[Vector] = {
    dataset.map(r => this.transform(r))
  }
*/

  def genericMap[T: ClassTag: TypeInformation](dataset: DataSet[T]): DataSet[T] = {
    dataset.map(r => r)
  }

}

object HashingTrick {

  private[entityrank] val Native: String = "native"

  /**
   * Native Scala hash code implementation.
   */
  private[entityrank] def nativeHash(term: Any): Int = term.##

}
