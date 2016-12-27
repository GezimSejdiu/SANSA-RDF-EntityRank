package net.sansa.entityrank.spark.utils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.{ Map => JMap }

object SparkUtils {

  /** Specify the master for Spark*/
  //final val SPARK_MASTER:String = "spark://spark-master:7077";
  final val SPARK_MASTER: String = "local[2]";
  final val getHDFSPath: String = "hdfs://"

  def createSparkConf(master: String, jobName: String, sparkHome: String, jars: Array[String],
    environment: JMap[String, String]): SparkConf = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(jobName)
      .setJars(jars)
    conf.set("spark.kryo.registrator", System.getProperty("spark.kryo.registrator", "net.sansa.entityrank.spark.utils.Registrator"))
  }

  def createSparkContext(master: String, jobName: String, sparkHome: String, jars: Array[String],
    environment: JMap[String, String]): SparkContext = {
    val conf = createSparkConf(master, jobName, sparkHome, jars, environment)
    new SparkContext(conf)
  }

  def getSparkContext(): SparkContext = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark-RDF-EntityRank")
    conf.setSparkHome("/opt/spark-1.5.1")
    conf.setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryoserializer.buffer.max", "512")
    //conf.set("spark.kryo.registrator", System.getProperty("spark.kryo.registrator", "net.sansa.entityrank.spark.utils.Registrator"))
    conf.set("spark.akka.frameSize", "128")
    conf.set("spark.storage.memoryFraction", "0.5")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "net.sansa.entityrank.spark.utils.Registrator")

    //.set("spark.executor.memory", "2g")
    new SparkContext(conf)
  }

  /**
   * Set all loggers to the given log level.  Returns a map of the value of every logger
   * @param level
   * @param loggers
   * @return
   */
  def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) =
    {
      loggers.map {
        loggerName =>
          val logger = org.apache.log4j.Logger.getLogger(loggerName)
          val prevLevel = logger.getLevel()
          logger.setLevel(level)
          loggerName -> prevLevel
      }.toMap
    }

  //setLogLevels(org.apache.log4j.Level.WARN, Seq("org.apache", "flink", "org.eclipse.jetty", "akka"))

}