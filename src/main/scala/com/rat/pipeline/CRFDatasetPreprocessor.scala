package com.rat.pipeline

import com.databricks.spark.corenlp.functions._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Hardik on 10/14/2016.
  */
object CRFDatasetPreprocessor {

  def fetchReviews(dataset: RDD[String]): RDD[String] = {
    dataset
      .filter(line => !line.contains("[t]"))
      .filter(line => line.length() > 0)
      .map(line => if (line.indexOf("##") == 0) line.substring(line.lastIndexOf("##") + 1)
      else {
        val temp = line.split("##")
        temp(1)
      })
      .map(line => if (line.charAt(0) == '#') line.substring(1) else line).filter(line => line.length() > 0)
  }

  def fetchAspects(dataset: RDD[String]): String = {
    dataset
      .filter(line => !line.contains("[t]"))
      .filter(line => line.length() > 0)
      .filter(line => line.indexOf("##") > 1)
      .map(line => {
        val parts = line.split("##")
        val aspectsWSenti = parts(0).split(",")
        val tempArray: ArrayBuffer[String] = new ArrayBuffer[String]
        aspectsWSenti.foreach(aspect => {
          if (!tempArray.contains(aspect.replaceAll("\\[.\\w{1,2}\\](\\[.\\])?", "").trim))
            tempArray.append(aspect.replaceAll("\\[.\\w{1,2}\\](\\[.\\])?", "").trim.toLowerCase)
        })
        tempArray.mkString(",")
      }).reduce(_ + "," + _)
  }

  def prepareDataset(sparkContext: SparkContext, reviewFile: String, trainFile: String, testFile: String): Unit = {
    //    fetchAspectsFromDataset(sparkContext, reviewFile, extractedAspectsFile)
    //    fetchReviewsFromDataset(sparkContext, reviewFile, extractedReviewsFile)

    val datasetRdd = sparkContext.textFile(reviewFile)

    val reviewTextRdd = fetchReviews(datasetRdd)

    //    val sparkSession = new SparkSession.Builder().config("spark.sql.warehouse.dir", "file:///D://spark242//").getOrCreate()
    val sparkSession = new SparkSession.Builder().master("yarn").getOrCreate()
    //    val sparkSession = new SparkSession.Builder().getOrCreate()
    val reviewTextSchema = StructType(Seq(StructField("text", StringType, true)))
    val reviewSourceRDD = reviewTextRdd.map(line ⇒ Row(line))

    val reviewTextDF = sparkSession.createDataFrame(reviewSourceRDD, reviewTextSchema)
    val featureDF = reviewTextDF.select(col("text"), pos(col("text")), tokenize(col("text")), depparse(col("text"))).toDF("text", "pos", "token", "dep")
    val featureRDD = featureDF.rdd

    val aspectsArray: Array[String] = fetchAspects(datasetRdd).split(",")

    val resultDF = featureRDD.map {
      row => {
        //      Get Review Text with following line
        //      val reviewText = row.get(0)
        val posTags = row.getAs[Seq[String]]("pos")
        val tokens = row.getAs[Seq[String]]("token")

        val depTreeItems: ArrayBuffer[String] = new ArrayBuffer[String]()
        val depTree = row.getAs[Seq[Row]]("dep")
        depTree.foreach(item => {
          depTreeItems.append(item.mkString("\t"))
        })

        val array: ArrayBuffer[String] = new ArrayBuffer[String]
        var nextTokenPOS: String = ""
        var previousTokenPOS: String = ""

        for (i <- tokens.indices) {
          var token: String = tokens(i)
          val iobTag = if (aspectsArray.count(_.equalsIgnoreCase(token)) > 0) "I" else "O"
          val tokenPOS: String = posTags(i)
          var previousToken: String = "null"
          var nextToken: String = "null"
          var headTokenRelation: String = "null"
          var depTokenRelation: String = "null"
          var headTokenIndex: Int = -1
          val headRelationsArray: ArrayBuffer[String] = new ArrayBuffer[String]
          val depRelationsArray: ArrayBuffer[String] = new ArrayBuffer[String]
          depTreeItems.foreach(item => {

            val itemValues: Array[String] = item.split("\t")

            token = token.replace("[", "").replace("]", "")

            // Check to see if current token have any dependent words
            if (itemValues(0).contains(token)) {
              if (itemValues(2).equals("amod") || itemValues(2).equals("nsubj") || itemValues(2).equals("dep"))
                headRelationsArray.append(itemValues(2))
            }
            // Check to see if current token have any head words
            if (itemValues(3).contains(token)) {
              if (itemValues(2).equals("nsubj") || itemValues(2).equals("dobj") || itemValues(2).equals("dep"))
                depRelationsArray.append(itemValues(2))
              headTokenIndex = itemValues(1).toInt
            }
          })

          val headTokenPOS: String = if (headTokenIndex > 0) posTags(headTokenIndex - 1) else "null"

          if (i != 0) {
            previousToken = tokens(i - 1)
            previousTokenPOS = posTags(i - 1)
          }

          if (i != (tokens.size - 1)) {
            nextToken = tokens(i + 1)
            nextTokenPOS = posTags(i + 1)
          }

          headTokenRelation = if (headRelationsArray.length < 1) "null" else headRelationsArray.mkString(",")
          depTokenRelation = if (depRelationsArray.length < 1) "null" else depRelationsArray.mkString(",")
          // TODO Experiment with features here
          // TODO previousTokenPOS and nextTokenPOS can be added here

          //        array.append(token + "|" + tokenPOS + "|" + previousToken + "|" + nextToken + "|" + headTokenPOS + "|" + headTokenRelation + "|" + depTokenRelation + "|" + iobTag)
          //        array.append(token + "|" + tokenPOS + "|" + headTokenPOS + "|" + headTokenRelation + "|" + depTokenRelation + "|" + iobTag)
          //        array.append(token + "|" + headTokenPOS + "|" + headTokenRelation + "|" + depTokenRelation + "|" + iobTag)
          val headToken = if (headTokenIndex != -1) tokens(headTokenIndex - 1) else "null"
          array.append(token + "|" + tokenPOS + "|" + headToken + "|" + headTokenRelation + "|" + depTokenRelation + "|" + iobTag)
          //        array.append(headTokenPOS + "|" + headTokenRelation + "|" + depTokenRelation + "|" + iobTag)
        }

        val newRow: Row = Row.fromSeq(array)
        newRow
      }
    }

    val split = resultDF.randomSplit(Array(0.8, 0.2))

    split(0).map(row => row.mkString("\t")).saveAsObjectFile(trainFile)
    split(1).map(row => row.mkString("\t")).saveAsObjectFile(testFile)

    //    val split = resultDF.randomSplit(Array(0.2, 0.2, 0.2, 0.2, 0.2))
    //
    //    Set for 5-fold cross validations
    //    split(0).union(split(1)).union(split(2)).union(split(3)).map(row => row.mkString("\t")).saveAsObjectFile(trainFile + "1")
    //    split(4).map(row => row.mkString("\t")).saveAsObjectFile(testFile + "1")
    //    split(1).union(split(2)).union(split(3)).union(split(4)).map(row => row.mkString("\t")).saveAsObjectFile(trainFile + "2")
    //    split(0).map(row => row.mkString("\t")).saveAsObjectFile(testFile + "2")
    //    split(2).union(split(3)).union(split(4)).union(split(0)).map(row => row.mkString("\t")).saveAsObjectFile(trainFile + "3")
    //    split(1).map(row => row.mkString("\t")).saveAsObjectFile(testFile + "3")
    //    split(3).union(split(4)).union(split(0)).union(split(1)).map(row => row.mkString("\t")).saveAsObjectFile(trainFile + "4")
    //    split(2).map(row => row.mkString("\t")).saveAsObjectFile(testFile + "4")
    //    split(4).union(split(0)).union(split(1)).union(split(2)).map(row => row.mkString("\t")).saveAsObjectFile(trainFile + "5")
    //    split(3).map(row => row.mkString("\t")).saveAsObjectFile(testFile + "5")

    // Set for 5-fold cross validations
    //    split(0).++(split(1).++(split(2).++(split(3)))).map(row => row.mkString("\t")).saveAsObjectFile(trainFile + "1")
    //    split(4).map(row => row.mkString("\t")).saveAsObjectFile(testFile + "1")
    //    split(1).++(split(2).++(split(3).++(split(4)))).map(row => row.mkString("\t")).saveAsObjectFile(trainFile + "2")
    //    split(0).map(row => row.mkString("\t")).saveAsObjectFile(testFile + "2")
    //    split(2).++(split(3).++(split(4).++(split(0)))).map(row => row.mkString("\t")).saveAsObjectFile(trainFile + "3")
    //    split(1).map(row => row.mkString("\t")).saveAsObjectFile(testFile + "3")
    //    split(3).++(split(4).++(split(0).++(split(1)))).map(row => row.mkString("\t")).saveAsObjectFile(trainFile + "4")
    //    split(2).map(row => row.mkString("\t")).saveAsObjectFile(testFile + "4")
    //    split(4).++(split(0).++(split(1).++(split(2)))).map(row => row.mkString("\t")).saveAsObjectFile(trainFile + "5")
    //    split(3).map(row => row.mkString("\t")).saveAsObjectFile(testFile + "5")
    //    resultDF.map(row => row.mkString("\t")).saveAsTextFile(trainFile)

  }

  def fetchReviewsFromDataset(sparkContext: SparkContext, reviewFile: String, extractedReviewsFile: String) = {

    val datasetRdd = sparkContext.textFile(reviewFile).cache()

    val reviewTextRdd: RDD[String] = datasetRdd
      .filter(line => !line.contains("[t]"))
      .filter(line => line.length() > 0)
      .map(line => if (line.indexOf("##") == 0) line.substring(line.lastIndexOf("##") + 1)
      else {
        val temp = line.split("##")
        temp(1)
      })
      .map(line => if (line.charAt(0) == '#') line.substring(1) else line).filter(line => line.length() > 0)

    //Save RDD as text file here
    reviewTextRdd.saveAsTextFile(extractedReviewsFile)

  }

  def fetchAspectsFromDataset(sparkContext: SparkContext, reviewFile: String, extractedAspectsFile: String) = {

    val datasetRdd = sparkContext.textFile(reviewFile).cache()

    val multiAspectString: String = datasetRdd
      .filter(line => !line.contains("[t]"))
      .filter(line => line.length() > 0)
      .filter(line => line.indexOf("##") > 1)
      .map(line => {
        val parts = line.split("##")
        val aspectsWSenti = parts(0).split(",")
        val tempArray: ArrayBuffer[String] = new ArrayBuffer[String]
        aspectsWSenti.foreach(aspect => {
          if (!tempArray.contains(aspect.replaceAll("\\[.\\w{1,2}\\](\\[.\\])?", "").trim))
            tempArray.append(aspect.replaceAll("\\[.\\w{1,2}\\](\\[.\\])?", "").trim.toLowerCase)
        })
        tempArray.mkString(",")
      }).reduce(_ + "," + _)

    val aspectRdd: RDD[String] = sparkContext.makeRDD(multiAspectString.split(","))

    //Save Aspects RDD as text file here
    aspectRdd.distinct().saveAsTextFile(extractedAspectsFile)

  }

  def countAspectForGivenReview(sparkContext: SparkContext, reviewFile: String, reviewSentence: String): Long = {
    val datasetRdd = sparkContext.textFile(reviewFile).cache()
    val aspectCount: Long = datasetRdd
      .filter(line => !line.contains("[t]"))
      .filter(line => line.length() > 0)
      .filter(line => line.indexOf("##") > 1)
      .map(line => {
        val parts = line.split("##")
        if (parts(1).equalsIgnoreCase(reviewSentence)) {
          parts(0).split(",").length
        }
      }).count()

    aspectCount

  }
}
