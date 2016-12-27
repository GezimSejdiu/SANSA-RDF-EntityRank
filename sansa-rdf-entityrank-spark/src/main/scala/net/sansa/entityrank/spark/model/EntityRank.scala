package net.sansa.entityrank.spark.model

import com.hp.hpl.jena.graph.{ Node => JNode }
import org.apache.spark.rdd.RDD

class EntityRank extends IRank {

  override def rank(triples: RDD[Triples]): RDD[(JNode, Double)] = {
    triples.map {
      case Triples(s, p, o) => (s, o.getIndexingValue().toString().toDouble)
    }
  }

  def rank_DEPTH(triples: RDD[Triples]): RDD[(JNode, Double)] = {
    triples.map {
      case Triples(s, p, o) => (s, 0.0)
    }
  }
}