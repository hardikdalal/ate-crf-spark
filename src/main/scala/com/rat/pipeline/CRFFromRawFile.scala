package com.rat.pipeline

import com.intel.ssg.bdt.nlp.{CRF, CRFModel, Sequence, Token}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Hardik on 10/28/2016.
  */
object CRFFromRawFile {
  def runModel(sparkContext: SparkContext, trainFile: String, testFile: String, resultFile: String, processedTestFile: String, templateFile: String) {

    val templates: Array[String] = scala.io.Source.fromFile(templateFile).getLines().filter(_.nonEmpty).toArray


    val trainRDD: RDD[Sequence] = sparkContext.objectFile[String](trainFile)
      .filter(_.nonEmpty).map(sentence => {
      val tokens = sentence.split("\t")
      Sequence(tokens.map(token => {
        val tags: Array[String] = token.split('|')
        Token.put(tags.last, tags.dropRight(1))
      }))
    })

    val model: CRFModel = CRF.train(templates, trainRDD, 0.25, 2)

    val testRDDWithoutLabel: RDD[Sequence] = sparkContext.objectFile[String](testFile).filter(_.nonEmpty).map(sentence => {
      val tokens = sentence.split("\t")
      Sequence(tokens.map(token => {
        val tags = token.split('|')
        Token.put(tags.dropRight(1))
      }))
    })

    sparkContext.objectFile[String](testFile)
      .filter(_.nonEmpty)
      .map(sentence => {
        val tokens = sentence.split("\t")
        Sequence(tokens.map(token => {
          val tags = token.split('|')
          Token.put(tags.last, tags.dropRight(1))
        }))
      }).saveAsObjectFile(processedTestFile)


    val results = model.predict(testRDDWithoutLabel)

    results.saveAsObjectFile(resultFile)
  }


  def runModel1(sparkContext: SparkContext, trainFile: String, testFile: String, resultFile: String, processedTestFile: String, templateFile: String) {

    val templates: Array[String] = scala.io.Source.fromFile(templateFile).getLines().filter(_.nonEmpty).toArray

    for (i <- 1.to(5)) {
      println("Training model on fold - " + i + " currently")

      val trainRDD: RDD[Sequence] = sparkContext.objectFile[String](trainFile + i)
        .filter(_.nonEmpty).map(sentence => {
        val tokens = sentence.split("\t")
        Sequence(tokens.map(token => {
          val tags: Array[String] = token.split('|')
          Token.put(tags.last, tags.dropRight(1))
        }))
      })

      val model: CRFModel = CRF.train(templates, trainRDD, 0.25, 2)

      val testRDDWithoutLabel: RDD[Sequence] = sparkContext.objectFile[String](testFile + i).filter(_.nonEmpty).map(sentence => {
        val tokens = sentence.split("\t")
        Sequence(tokens.map(token => {
          val tags = token.split('|')
          Token.put(tags.dropRight(1))
        }))
      })

      sparkContext.objectFile[String](testFile + i)
        .filter(_.nonEmpty)
        .map(sentence => {
          val tokens = sentence.split("\t")
          Sequence(tokens.map(token => {
            val tags = token.split('|')
            Token.put(tags.last, tags.dropRight(1))
          }))
        }).saveAsObjectFile(processedTestFile + i)


      val results = model.predict(testRDDWithoutLabel)

      results.saveAsObjectFile(resultFile + i)
    }
  }

}
