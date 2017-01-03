package net.sansa_stack.entityrank.spark.model

import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import scala.reflect.runtime.universe
import org.apache.spark.sql.SQLContext

/*
 * Gezim Sejdiu
 * Methods for pipeline transformations over DataFrames.
 */
object TriplesDataFrame {

  def transfromEntities(df: DataFrame, colName: String, n_gramConst: Int = 3, numTextFeatures: Int = 1000): DataFrame = {

    val n_gram = new NGram().setN(n_gramConst).setInputCol("filteredWords").setOutputCol("ngrams")
    val hashTF = new HashingTF().setInputCol("ngrams").setOutputCol("rowFeatures").setNumFeatures(numTextFeatures)

    //fit the transformation to the data
    val filteredWords = tokenize(df, colName, "filteredWords")
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
  
   def TFIDF(df: DataFrame, inputColName: String, numFeatures: Int, minDocFreq: Int): (DataFrame, IDFModel) = {
    // define the transformations
    val hasher = new HashingTF().setInputCol(inputColName).setOutputCol("rawFeatures").setNumFeatures(numFeatures)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features").setMinDocFreq(minDocFreq)

    // map the transformations into the data
    val hashed = hasher.transform(df)
    val idfModel = idf.fit(hashed)

    (idfModel.transform(hashed), idfModel)
  }

  val wordCounts = udf((tokens: Seq[String]) => tokens.foldLeft(Map.empty[String, Int]) {
    (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
  }.toSeq.sortBy(-_._2))
  
  /*
   *  // $example on$
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show()
   // $example on$
    val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("id", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

    val countTokens = udf { (words: Seq[String]) => words.length }

    val tokenized = tokenizer.transform(sentenceDataFrame)
    tokenized.select("sentence", "words")
        .withColumn("tokens", countTokens(col("words"))).show(false)

    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
    regexTokenized.select("sentence", "words")
        .withColumn("tokens", countTokens(col("words"))).show(false)
    // $example off$

    spark.stop()
    
    // $example on$
    // Load the data stored in LIBSVM format as a DataFrame.
    val data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 1234L)

    // Train a NaiveBayes model.
    val model = new NaiveBayes()
      .fit(trainingData)

    // Select example rows to display.
    val predictions = model.transform(testData)
    predictions.show()

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test set accuracy = " + accuracy)
    // $example off$
     * 
   * 
   */

}