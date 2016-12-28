package net.sansa_stack.entityrank.spark.utils

import org.apache.spark.ml.feature._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/*
 * Gezim Sejdiu
 * Methods for pipeline transformations over DataFrames.
 */
object PipelineTransformations {

  def transfromEntities(df: DataFrame, colName: String, n_gramConst: Int = 3, numTextFeatures: Int = 1000)(implicit config: SparkSession): DataFrame = {

    val n_gram = new NGram().setN(n_gramConst).setInputCol("words").setOutputCol("ngrams")
    val hashTF = new HashingTF().setInputCol("ngrams").setOutputCol("rowFeatures").setNumFeatures(numTextFeatures)

    //fit the transformation to the data
    val filteredWords = tokenize(df, colName, "words")
    val ngrams = n_gram.transform(filteredWords)
    val filteredWordsWithCounts = ngrams.withColumn("wordCounts", wordCounts(ngrams("ngrams")))

    hashTF.transform(filteredWordsWithCounts)
  }

  def tokenize(df: DataFrame, inputColName: String, outputColName: String): DataFrame = {
    // define the transformations
    val tokenizer = new RegexTokenizer().setInputCol(inputColName).setOutputCol("words").setPattern("\\w+").setGaps(false)
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol(outputColName)

    // map the transformations into the data
    val wordsData = tokenizer.transform(df)

    remover.transform(wordsData).drop("words")
  }

  val wordCounts = udf((tokens: Seq[String]) => tokens.foldLeft(Map.empty[String, Int]) {
    (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
  }.toSeq.sortBy(-_._2))

}