package net.sansa_stack.entityrank.spark.io

import org.apache.spark.rdd.RDD
import net.sansa_stack.entityrank.spark.utils.Logging
import net.sansa_stack.entityrank.spark.model.Triples

/**
 * Writes triples to disk.
 *
 * @author Gezim Sejdiu
 *
 */

object TripleWriter extends Logging {

  def writeToFile(triples: RDD[Triples], path: String) = {
    val startTime = System.currentTimeMillis()

    triples
      .map(t => "<" + t.subj.getLiteral() + "> <" + t.pred.getLiteral() + "> <" + t.obj.getLiteral() + "> .")
      .saveAsTextFile(path)
  }

}