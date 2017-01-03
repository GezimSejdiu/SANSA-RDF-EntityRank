package net.sansa_stack.entityrank.spark.ranking

import com.hp.hpl.jena.graph.{ Node => JNode }
import org.apache.spark.rdd.RDD
import net.sansa_stack.entityrank.spark.model.Triples
import org.apache.spark.sql.{SparkSession, DataFrame }
import net.sansa_stack.entityrank.spark.model.TriplesDataFrame._
import net.sansa_stack.entityrank.spark.nlp.StanfordAnnotation
import org.apache.spark.sql.functions.udf

class EntityRank extends IRank {

  override def rank(triples: RDD[Triples]): RDD[(JNode, Double)] = {
    triples.map {
      case Triples(s, p, o) => (s, o.getIndexingValue().toString().toDouble)
    }
  }

  def rank_DEPTH(triples: RDD[Triples]): RDD[(JNode, Double)] = {
    triples.map {
      case Triples(s, p, o) => (s, 0.0)
    }
  }
}

/**
 * Implicit mapper for EntityRank dataframe
 */
object EntityRankDataFrame {

  implicit class EntityRankDF(val data: DataFrame) {
    def rescale(): DataFrame = {

      // Split the data into training and test sets (30% held out for testing)
      val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), 1234L) //seed = 1234L)

      val tokenizedTrainingData = tokenize(trainingData, "entities", "tokens")
      val tokenizedTestData = tokenize(testData, "entities", "tokens")

      val (rescaledCorpus, idfModel) = TFIDF(tokenizedTrainingData, "tokens", 100000, 2)
      rescaledCorpus
    }

    def highestRankedResources()(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      data.groupBy($"entities").avg("rank").sort($"avg(rank)".desc)
    }

    def generateRanking()(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      data
        .sample(withReplacement = true, 0.001, 1234)
        .withColumn("EntitySentiment", getSentiment($"entity"))
    }
  }

  //udf function for sentiment anotation
  val sentimentArg: String => String = (param: String) => StanfordAnnotation.Sentiment(param)
  val getSentiment = udf(sentimentArg)
}
