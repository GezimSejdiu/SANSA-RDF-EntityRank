package net.sansa.entityrank.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
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

object App extends Logging {

  val SPARK_MASTER = "spark://139.18.2.34:3077" //"spark://139.18.2.34:3077"//"spark://gezim-Latitude-E5550:7077" //

  def main(args: Array[String]): Unit = {

    // val sparkConf = new SparkConf()
    /*
      //.setMaster("spark://gezim-Latitude-E5550:7077")
      .setMaster(SPARK_MASTER)
      .setAppName("Spark-RDF-EntityRank")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "512")
      //.set("spark.executor.memory", "2g")
      //.set("spark.cores.max", args(0))
*/
    //val sparkContext = new SparkContext(sparkConf)
    SparkUtils.setLogLevels(org.apache.log4j.Level.OFF, Seq("org.apache", "spark", "org.eclipse.jetty", "akka", "org"))
    implicit val sparkContext = SparkUtils.getSparkContext()
    //sparkContext.setLogLevel("WARN")
    //sparkContext.addJar("/home/gezim/workspace/SparkProjects/Spark-RDF-Statistics/target/Spark-RDF-Statistics-0.0.1-SNAPSHOT-jar-with-dependencies.jar")

    //val file = "/opt/spark/data/tests/page_links_simple.nt.bz2"
    val file = "/opt/spark-1.5.1/nyseSimpl_copy.nt"
    //val file = args(1)
    //val outputPath = args(1)
    ////val file = "hdfs://akswnc5.informatik.uni-leipzig.de:54310/gsejdiu/DistLODStats/LinkedGeoData/"
    //hdfsfile + "/gsejdiu/DistLODStats/Dbpedia/en/page_links_en.nt.bz2")
    // val file = "hdfs://akswnc5.informatik.uni-leipzig.de:54310/gsejdiu/DistLODStats//Dbpedia/en/page_links_en.nt.bz2"
    logger.info("Runing RDF-EntityRank....")
    val startTime = System.currentTimeMillis()

    // load triples
    // val triples = TripleReader.loadFromFile(file, sparkContext, 2)
    // val T = sparkContext.textFile(file).map{line => val field:Array[String]=line.split(" "); (field(0),field(1),field(2))}.cache;

    // triples.take(5).foreach(println(_))

    // compute  criterias
    // val rdf_statistics = new EntityRank

    // write triples on disk
    //TripleWriter.writeToFile(rdf_statistics, outputPath)
    // println("finished loading " + triples.count() + " triples in " + (System.currentTimeMillis() - startTime) + "ms.")

    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._

    val triples = TripleReader.loadSFromFile(file, sparkContext, 2) //.toDF("Subject", "Predicate", "Object")
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

    //val DFtriples = triples.toDF("words")
    val s = vtriples.map(Tuple1.apply).toDF("words")

    /*   val VV = triples.map { v =>

     // val label = v._1
      val words = v.toString.split(",").toSeq
      //(label, words)
      words
      //(label, words)=> (v._1, v.toString.split(",").toSeq)
    }.toDF("words")*/

    val documentDF = sqlContext.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")).map(Tuple1.apply)).toDF("words")

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
    val trans = PipelineTransformations.transfromEntities(documentDF, "words")
   // val toc = PipelineTransformations.tokenize(documentDF, "words", "result")
    
    trans.collect().foreach(println(_))

   // val ngramDataFrame = PipelineTransformations.transfromEntities(documentDF, "words")
    //ngramDataFrame.foreach(println(_))

    //ngramDataFrame.take(3).map(_.getAs[Stream[String]]("ngrams").toList).foreach(println)

    //println(stat.mean) // a dense vector containing the mean value for each column
    //println(stat.variance) // column-wise variance
    //println(stat.numNonzeros) // number of nonzeros in each column

    //val tf = hashTF.transform(triples)

    //tf.take(3).foreach(println(_))

    //val documents: RDD[Seq[String]] = triples.map(t => t.toString().split(" ").toSeq).toDF()

    /*
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] =hashingTF.transform(documents)

    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("features", "label").take(3).foreach(println)
*/
    sparkContext.stop()
  }

  // definition of program arguments
  case class Params(
    appName: String = "Spark-RDF-EntityRank",
    master: String = null,
    numIters: Int = 10,
    fromFile: Boolean = false,
    linksPath: String = null,
    resultPath: String = null)

  def argumentsParser: OptionParser[Params] = new OptionParser[Params]("PageRank") {
    head("Spark-RDF-EntityRank....")
    opt[String]("app_name")
      .text("Applicaton name")
      .action((x, c) => c.copy(appName = x))
    opt[Int]("num_iters")
      .text("Num of iterations")
      .action((x, c) => c.copy(numIters = x))
    opt[String]("links_file")
      .text("File containing the links of each entity.")
      .required()
      .action((x, c) => c.copy(linksPath = x))
    opt[String]("result_path")
      .text("Path to the pagerank results.")
      .required()
      .action((x, c) => c.copy(resultPath = x))
  }

  /*
    def main(args: Array[String]) {
    argumentsParser.parse(args, Params()) map {
      case params: Params =>
        run(params)
    } getOrElse {
      sys.exit(1)
    }
    
  }
  */

} 
