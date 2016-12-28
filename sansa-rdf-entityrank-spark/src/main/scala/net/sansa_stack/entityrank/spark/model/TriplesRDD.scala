package net.sansa_stack.entityrank.spark.model

import org.apache.spark.rdd.RDD
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.graph.Node_Literal
import com.hp.hpl.jena.vocabulary.RDF
import net.sansa_stack.entityrank.spark.utils.Prefixes

/**
 * A data structure that comprises a set of triples.
 *
 * @author Gezim Sejdiu
 *
 */
case class TriplesRDD(triples: RDD[Triples]) {

  /**
   * Returns the number of triples.
   *
   * @return the number of triples
   */
  def size() = {
    triples.count()
  }

  /**
   * Returns a RDD of triples
   *
   * @return RDD of triples
   */
  def getTriples = triples

  /**
   * Returns a RDD of subjects
   *
   * @return RDD of subjects
   */
  def getSubjects = triples.map(_.getSubject)

  /**
   * Returns a RDD of predicates
   *
   * @return RDD of predicates
   */
  def getPredicates = triples.map(_.getPredicate)

  /**
   * Returns a RDD of objects
   *
   * @return RDD of objects
   */
  def getObjects = triples.map(_.getObject)

  /**
   * Returns a RDD of Literals
   *
   * @returns RDD of literals
   */

  def getLiterals(rdd: RDD[Triples]) = rdd.map(_.obj).flatMap {
    case l: Node_Literal => Some(l);
    case _               => None
  }

  def getTypes(rdd: RDD[Triples]) = rdd.flatMap {
    case Triples(sub, pred, obj) if RDF.`type`.equals(pred) => Some(obj); case _ => None
  }

  def isLiteral = (n: Node) => n match { case l: Node_Literal => true; case _ => false }
  def toLiteral = (n: Node) => n match { case l: Node_Literal => l }
  def isLanguage(lang: String) = (l: Node_Literal) => l.getLiteralLanguage match { case `lang` => true; case _ => false }

  def typeJoin = {
    getTriples.cache()
    val typePair = getTriples
      .filter { case Triples(sub, pred, obj) => RDF.`type`.equals(pred.toString()) }
      .map { case Triples(sub, pred, obj) => (sub, obj) }

    val predPair = getTriples
      .filter { case Triples(sub, pred, obj) => !RDF.`type`.equals(pred.toString()) }
      .map { case Triples(sub, pred, obj) => (sub, pred) }

    typePair
      .join(predPair)
      .map { case (k, v) => v }
      .groupByKey()
  }

}