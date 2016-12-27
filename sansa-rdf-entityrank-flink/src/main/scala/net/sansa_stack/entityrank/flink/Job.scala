package net.sansa_stack.entityrank.flink

import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import java.util.Properties
import net.sansa_stack.entityrank.flink.utils.{Logging, FlinkSettings}
import net.sansa_stack.entityrank.flink.io.{TripleReader,StringInputStream}
import net.sansa_stack.entityrank.flink.feature.HashingTrick
import org.apache.flink.ml.math.Vector
import net.sansa_stack.entityrank.flink.feature._

object Job extends Logging {
  def main(args: Array[String]) {

    // parse parameters
    val params = ParameterTool.fromArgs(args)
    //val input = params.getRequired("input")
    //val ouptut = params.getRequired("input")

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    FlinkSettings.setLogLevels(org.apache.log4j.Level.WARN, Seq("org.apache", "flink", "org.eclipse.jetty", "akka"))

    logger.info("Runing RDF-EntityRank....")
    val startTime = System.currentTimeMillis()

    //val triples = TripleReader.loadFromFile("hdfs://akswnc5.informatik.uni-leipzig.de:54310/gsejdiu/DistLODStats/BSBM/BSBM_2GB.nt", env)
   
    val fn = "/opt/spark-1.5.1/nyseSimpl_copy.nt"
    //val fn = "/opt/spark/data/tests/page_links_simple.nt"

    val triples = TripleReader.loadSFromFile(fn, env)

    val vtriples = triples.map(t => t.toString.split(",").toIterable)
    
    
    val hashTF = new HashingTrick()
    //Transforms the input document to term frequency vectors.
    val tf: DataSet[Vector] = vtriples.map(f => (hashTF.transform(f)))
    
    // Fit the model to the data.
    val idf = new IDF().fit(tf)
    val tfidf: DataSet[Vector] = idf.transform(tf)
    
    val ddd = new TFIDF
    ddd.fit(tf)
    
    tf.collect.take(3).foreach(println(_))
    tf.print

    println("finished loading " + triples.count() + " triples in " + (System.currentTimeMillis() - startTime) + "ms.")

    //TripleWriter.writeToFile(triples, "/home/gezim/Desktop/SDA_Bonn/")

    //env.execute();
  }
}
