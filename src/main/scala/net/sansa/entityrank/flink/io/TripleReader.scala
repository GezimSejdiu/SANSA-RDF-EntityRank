package net.sansa.entityrank.flink.io

import java.io.InputStream
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.scala._
import net.sansa.entityrank.flink.utils.Logging
import net.sansa.entityrank.flink.model.Triples
import org.openjena.riot.RiotReader
import org.openjena.riot.Lang
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

  def loadFromFile(path: String, env: ExecutionEnvironment): DataSet[Triples] = {
    val triples =
      env.readTextFile(path)
        .filter(line => !line.trim().isEmpty & !line.startsWith("#"))
        .map(f => parseTriples(path))
    triples
  }

  def loadSFromFile(path: String, env: ExecutionEnvironment): DataSet[(String, String, String)] = {
    logger.info("loading triples from disk...")
    val startTime = System.currentTimeMillis()

    val tr =
      env.readTextFile(path)
        .filter(line => !line.trim().isEmpty & !line.startsWith("#"))

    val triples = tr
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
