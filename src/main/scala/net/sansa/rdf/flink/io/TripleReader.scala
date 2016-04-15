package net.sansa.rdf.flink.io

import org.openjena.riot.RiotReader
import org.openjena.riot.Lang
import java.io.InputStream
import net.sansa.rdf.flink.utils.Logging
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.DataSet
import net.sansa.rdf.flink.model.Triples
import org.apache.flink.api.scala._

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
