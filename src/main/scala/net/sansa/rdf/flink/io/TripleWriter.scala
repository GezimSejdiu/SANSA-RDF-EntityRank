package net.sansa.rdf.flink.io

import net.sansa.rdf.flink.utils.Logging
import org.apache.flink.api.scala._
import net.sansa.rdf.flink.model.Triples

/**
 * Writes triples to disk.
 *
 * @author Gezim Sejdiu
 *
 */

object TripleWriter extends Logging {

  def writeToFile(triples: DataSet[Triples], path: String) = {
    triples.map { t =>
      "<" + t.subj.getLiteral() + "> <" + t.pred.getLiteral() + "> <" + t.obj.getLiteral() + "> ."
    }.writeAsText(path)
  }
}