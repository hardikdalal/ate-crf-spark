package com.rat.pipeline

import com.intel.ssg.bdt.nlp.Token
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Hardik on 11/28/2016.
  */
object CRFEvaluation {
  def evaluate(sparkContext: SparkContext, resultFile: String, processedTestFile: String, experimentResultFile: String) {
    val gsAspectRDD = sparkContext.objectFile[String](processedTestFile)
      .map(sentence => {
        val tokens = sentence.split("\t")
        tokens.map(token => Token.deSerializer(token)).filter(token => token.label.contains("I"))
          .map(token => token.tags(0)).mkString("\n")
      }).filter(_.nonEmpty).distinct()

    //      gsAspectRDD.saveAsObjectFile(gsAspectFile + i)
    val extractedAspectRDD = sparkContext.objectFile[String](resultFile)
      .map(sentence => {
        val tokens = sentence.split("\t")
        tokens.map(token => Token.deSerializer(token)).filter(token => token.label.contains("I"))
          .map(token => token.tags(0)).mkString("\n")
      }).filter(_.nonEmpty).distinct()

    val scores = gsAspectRDD.intersection(extractedAspectRDD).count()
    val precision = scores / extractedAspectRDD.count()
    val recall = scores / gsAspectRDD.count()
    val fMeasure = 2 * ((precision * recall) / (precision + recall))

    sparkContext.makeRDD(Seq("Precision " + precision.toString, "Recall " + recall.toString, "F Measure " + fMeasure.toString)).saveAsTextFile(experimentResultFile)

  }

  def evaluate1(sparkContext: SparkContext, resultFile: String, processedTestFile: String, experimentResultFile: String) {

    val scores: ArrayBuffer[Float] = new ArrayBuffer[Float](5)
    val precision: ArrayBuffer[Float] = new ArrayBuffer[Float](5)
    val recall: ArrayBuffer[Float] = new ArrayBuffer[Float](5)
    val fMeasure: ArrayBuffer[Float] = new ArrayBuffer[Float](5)

    for (i <- 1.to(5)) {

      val gsAspectRDD = sparkContext.objectFile[String](processedTestFile + i)
        .map(sentence => {
          val tokens = sentence.split("\t")
          tokens.map(token => Token.deSerializer(token)).filter(token => token.label.contains("I"))
            .map(token => token.tags(0)).mkString("\n")
        }).filter(_.nonEmpty).distinct()

      //      gsAspectRDD.saveAsObjectFile(gsAspectFile + i)
      val extractedAspectRDD = sparkContext.objectFile[String](resultFile + i)
        .map(sentence => {
          val tokens = sentence.split("\t")
          tokens.map(token => Token.deSerializer(token)).filter(token => token.label.contains("I"))
            .map(token => token.tags(0)).mkString("\n")
        }).filter(_.nonEmpty).distinct()

      scores.append(gsAspectRDD.intersection(extractedAspectRDD).count())
      precision.append(scores(i - 1) / extractedAspectRDD.count())
      recall.append(scores(i - 1) / gsAspectRDD.count())
      fMeasure.append(2 * ((precision(i - 1) * recall(i - 1)) / (precision(i - 1) + recall(i - 1))))
    }

    precision.foreach(value => println("Precision = " + value))
    recall.foreach(value => println("Recall = " + value))
    fMeasure.foreach(value => println("FMeasure = " + value))

    sparkContext.makeRDD(Seq("Precision " + precision.mkString(","), "Recall " + recall.mkString(","), "F Measure " + fMeasure.mkString(","))).saveAsTextFile(experimentResultFile)

  }
}
