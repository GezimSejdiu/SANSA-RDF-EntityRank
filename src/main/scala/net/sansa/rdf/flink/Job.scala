package net.sansa.rdf.flink

import org.apache.flink.api.scala._
import net.sansa.rdf.flink.utils._
import org.apache.flink.api.java.utils.ParameterTool
import net.sansa.rdf.flink.io.TripleReader
import net.sansa.rdf.flink.io.TripleWriter
import java.util.Properties

object Job extends Logging {
  def main(args: Array[String]) {

    // parse parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")
    val ouptut = params.getRequired("output")

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    logger.info("start loading triples.....")
    val startTime = System.currentTimeMillis()

    val triples = TripleReader.loadFromFile(input, env)

    println("finished loading " + triples.count() + " triples in " + (System.currentTimeMillis() - startTime) + "ms.")

    TripleWriter.writeToFile(triples, ouptut)

    //env.execute();
  }
}
