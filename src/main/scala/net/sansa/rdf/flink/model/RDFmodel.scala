package net.sansa.rdf.flink.model

import java.io.InputStream
import com.hp.hpl.jena.rdf.model._

/**
 * ******************************************************************************
 *                                                                              *
 *                               Model                                     *
 *                                                                              *
 * ******************************************************************************
 */

class Node(val dt: String, val value: String, val lang: String) extends Serializable {
  override def toString: String = dt match {
    case "stringNode" => if (lang != null) "%s[%s]".format(value, lang) else value
    case "uriNode" => "<" + value + ">"
    case _ => value
  }
}

object Node {
  def apply(item: String) = {
    item.trim match {
      case x if (x.startsWith("<") && x.endsWith(">")) => new Node("uriNode", RDF.startDelimeter(x), null)
      case x if (x.startsWith("\"") && x.endsWith("\"")) => new Node("stringNode", RDF.startDelimeter(x), null)
      case x if (x.startsWith("_:")) => new Node("blankNode", x.substring(2), null)
      case x => new Node("null", "null", "null")
    }
  }
}

object RDF {

  def endDelimeter(line: String): String = {
    if (line.length() < 2 || line.takeRight(2) != " .") line else line.take(line.length() - 2)
  }

  def startDelimeter(item: String): String = if (item.length >= 2) item.substring(1, item.length - 1) else item
}

class newTriple(val subj: Node, val pred: Node, val obj: Node) extends Serializable {
  override def toString = subj + " " + pred + " " + obj
}

object newTriple {
  def apply(nt: String) = {
    val parts = RDF.endDelimeter(nt).split("\\s+", 3)
    new newTriple(Node(parts(0)), Node(parts(1)), Node(parts(2)))
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
