package net.sansa_stack.entityrank.flink.io

import org.apache.flink.api.scala._
import net.sansa_stack.entityrank.flink.utils.Logging
import net.sansa_stack.entityrank.flink.model.Triples

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