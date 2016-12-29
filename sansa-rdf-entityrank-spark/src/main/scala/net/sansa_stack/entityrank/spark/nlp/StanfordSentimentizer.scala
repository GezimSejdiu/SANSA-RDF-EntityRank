package net.sansa_stack.entityrank.spark.nlp

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.pipeline.{ Annotation, StanfordCoreNLP }
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentClass

object StanfordSentimentizer {

  val properties = new Properties()
  properties.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment")
  properties.setProperty("ssplit.isOneSentence", "true")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(properties)

  
  def Sentiment(sentence: String): String = {

    // create an empty Annotation just with the given text
    val document: Annotation = new Annotation(sentence)
    
    // run all Annotators on this text
    pipeline.annotate(document)

    val sentiment: String = document.get(classOf[SentencesAnnotation]).get(0).get(classOf[SentimentClass])

    sentiment
  }

}