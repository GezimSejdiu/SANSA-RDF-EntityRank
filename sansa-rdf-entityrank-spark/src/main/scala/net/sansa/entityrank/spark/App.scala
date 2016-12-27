package net.sansa.entityrank.spark

import scala.io.Source
import java.io.File
import org.apache.commons.io.FileUtils
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import scala.tools.nsc.io.Jar
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkConf
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION
import org.openjena.riot.RiotReader
import org.openjena.riot.Lang
import org.apache.spark.storage.StorageLevel
import net.sansa.entityrank.spark.utils._
import net.sansa.entityrank.spark.io.TripleReader
import net.sansa.entityrank.spark.model.EntityRank
import scopt.OptionParser
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

object App extends Logging {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println(
        "Usage: SANSA RDF EntityRank <input>")
      System.exit(1)
    }

    val input = args(0)

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple reader example (" + input + ")")
      .getOrCreate()

    SparkUtils.setLogLevels(org.apache.log4j.Level.OFF, Seq("org.apache", "spark", "org.eclipse.jetty", "akka", "org"))

    logger.info("Runing SANSA RDF-EntityRank....")
    val startTime = System.currentTimeMillis()

    val triples = TripleReader.loadSFromFile(input, sparkSession.sparkContext, 2) //.toDF("Subject", "Predicate", "Object")
    triples.take(5).foreach(println(_))
    val vtriples = triples.map(t => t.toString.split(",").toSeq)

    val hashTF = new HashingTF()
    val tf: RDD[Vector] = hashTF.transform(vtriples)
    tf.cache()
    tf.collect.take(3).foreach(println(_))

    // Fit the model to the data.
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    // val stat = Statistics.colStats(tfidf)

    tf.take(3).foreach(println(_))

    tfidf.take(3).foreach(println(_))
    import sparkSession.sqlContext.implicits._

    //val DFtriples = triples.toDF("words")
    val s = vtriples.map(Tuple1.apply).toDF("words")


    val documentDF = vtriples.toDF("words")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("words")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("result").take(3).foreach(println)

    val syn = model.findSynonyms("Spark", 3)
    val trans = PipelineTransformations.transfromEntities(documentDF, "words")(sparkSession)
    // val toc = PipelineTransformations.tokenize(documentDF, "words", "result")

    trans.collect().foreach(println(_))

    sparkSession.stop()
  }

} 
