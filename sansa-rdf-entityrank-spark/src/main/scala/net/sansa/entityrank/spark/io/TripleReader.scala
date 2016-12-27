package net.sansa.entityrank.spark.io

import org.openjena.riot.RiotReader
import org.openjena.riot.Lang
import org.apache.spark.SparkContext
import java.io.InputStream
import org.apache.spark.rdd.RDD
import net.sansa.entityrank.spark.utils.Logging
import net.sansa.entityrank.spark.model.Triples

/**
 * Reads triples.
 *
 * @author Gezim Sejdiu
 *
 */
object TripleReader extends Logging {

  def parseTriples(fn: String) = {
    val triples = RiotReader.createIteratorTriples(new StringInputStream(fn), Lang.NTRIPLES, "http://example/base").next
    Triples(triples.getSubject(), triples.getPredicate(), triples.getObject())
  }

  def loadFromFile(path: String, sc: SparkContext, minPartitions: Int = 2): RDD[Triples] = {

    val triples =
      sc.textFile(path) //, minPartitions) seems to be slower when we specify minPartitions
        .filter(line => !line.trim().isEmpty & !line.startsWith("#"))
        .map(parseTriples)
    triples
  }

  def loadSFromFile(path: String, sc: SparkContext, minPartitions: Int = 2): RDD[(String, String, String)] = {
    logger.info("loading triples from disk...")
    val startTime = System.currentTimeMillis()

    val triples =
      sc.textFile(path)
        .filter(line => !line.trim().isEmpty & !line.startsWith("#"))
        .map(line => line.replace(">", "").replace("<", "").split("\\s+")) // line to tokens
        .map(triple => (triple(0), triple(1), triple(2))) // tokens to triple

    logger.info("finished loading " + triples.count() + " triples in " + (System.currentTimeMillis() - startTime) + "ms.")
    triples
  }

}

class StringInputStream(s: String) extends InputStream {
  private val bytes = s.getBytes
  private var pos = 0

  override def read(): Int = if (pos >= bytes.length) {
    -1
  } else {
    val r = bytes(pos)
    pos += 1
    r.toInt
  }
}