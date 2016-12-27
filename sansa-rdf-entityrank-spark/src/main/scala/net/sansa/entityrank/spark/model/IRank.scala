package net.sansa.entityrank.spark.model

import com.hp.hpl.jena.graph.{ Node => JNode }
import org.apache.spark.rdd.RDD

/**
 * A data structure for a set of triples.
 *
 * @author Gezim Sejdiu
 *
 */

trait IRank{
  
  def rank(triples:RDD[Triples]):RDD[(JNode, Double)]
}