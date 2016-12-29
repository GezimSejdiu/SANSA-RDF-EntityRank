package net.sansa_stack.entityrank.spark.ranking

import com.hp.hpl.jena.graph.{ Node => JNode }
import org.apache.spark.rdd.RDD
import com.hp.hpl.jena.graph.{Node => JNode}
import net.sansa_stack.entityrank.spark.model.Triples

/**
 * A data structure for a set of triples.
 *
 * @author Gezim Sejdiu
 *
 */

trait IRank{
  
  def rank(triples:RDD[Triples]):RDD[(JNode, Double)]
}