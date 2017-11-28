package com.rat.pipeline

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Hardik on 10/30/2016.
  */
object Launch {

  def main(args: Array[String]): Unit = {


    val reviewFile = args(0)

    val validationPath = args(1)

    val templateFilePath = args(2)

    val evaluationFilePath = args(1) + "result"

    val trainFile = validationPath + "Train set"
    val testFile = validationPath + "Test set"
    val processedTestFile = validationPath + "Processed Test set"

    val resultFile = validationPath + "Result"

    val conf = new SparkConf().setMaster("yarn")
    val sparkContext = new SparkContext(conf)
    CRFDatasetPreprocessor.prepareDataset(sparkContext, reviewFile, trainFile, testFile)
    CRFFromRawFile.runModel(sparkContext, trainFile, testFile, resultFile, processedTestFile, templateFilePath)
    CRFEvaluation.evaluate(sparkContext, resultFile, processedTestFile, evaluationFilePath)

  }
}
