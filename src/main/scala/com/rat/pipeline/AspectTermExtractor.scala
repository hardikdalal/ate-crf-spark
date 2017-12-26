/*
  Title: Spark-based Aspect Term Extractor using CRFs
  Author: Hardik.Dalal@outlook.com
  Date: December 10, 2017
 */

import java.util.{List, Optional}
import com.intel.imllib.crf.nlp._
import edu.stanford.nlp.simple.Sentence
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer

object AspectTermExtractor {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = SparkContext.getOrCreate(conf)

    // Load the text file with reviews into a RDD
    val datasetRdd: RDD[String] = sc.textFile("/Users/hardikdalal/github/ate-crf-spark/Dataset.txt").cache()

    // Leave sentiment and reviews behind and build aspect term RDD from dataset
    val aspectRdd: RDD[(Long, String)] = datasetRdd
      .filter(line => !line.contains("[t]"))
      .filter(line => line.length() > 0)
      .filter(line => line.indexOf("##") > 1)
      .map(line => {
        val parts = line.split("##")
        val aspectsWSenti = parts(0).split(",")
        val tempArray: ArrayBuffer[String] = new ArrayBuffer[String]
        aspectsWSenti.foreach(aspect => {
          if (!tempArray.contains(aspect.replaceAll("\\[.\\w{1,2}\\](\\[.\\])?", "").trim))
            tempArray.append(aspect.replaceAll("\\[.\\w{1,2}\\](\\[.\\])?", "").trim)
        })
        tempArray.mkString(",")
      }).zipWithIndex().map(_.swap)

    // Leave sentiment and aspects behind and build CoreNLP.Sentence RDD from dataset
    val sentenceRdd: RDD[(Long, String)] = datasetRdd
      .filter(line => !line.contains("[t]"))
      .filter(_.nonEmpty) // to filter out empty lines in dataset
      .map(line => if (line.indexOf("##") != 0 && line.split("##").length > 1) line.split("##")(1) else line.substring(line.lastIndexOf("##") + 1))
      .map(line => if (line.charAt(0) == '#') line.substring(1) else line)
      .filter(_.nonEmpty) // to filter out empty lines due to previous map operations
      .zipWithIndex()
      .map(_.swap)

    // Build feature vector from sentence
    val featureVectorRdd: RDD[Sequence] = sentenceRdd
      .join(aspectRdd)
      .filter(_._2._1.length > 1) // to avoid one word reviews as CoreNLP cannot parse them
      .map(item => {
      val sentence = new Sentence(item._2._1) // Convert String to CoreNLP.Sentence
      val tokens: List[String] = sentence.words()
      val governors: List[Optional[Integer]] = sentence.governors()
      val posTags: List[String] = sentence.posTags()
      val dependencies: List[Optional[String]] = sentence.incomingDependencyLabels()
      val aspectTerm: ArrayBuffer[String] = new ArrayBuffer[String]()
      if (item._2._2.split(",").length > 0)
        aspectTerm.appendAll(item._2._2.split(","))
      val tokenBuffer: ArrayBuffer[Token] = new ArrayBuffer[Token]()
      for (i: Int <- 0 to tokens.size() - 1) {

        val strBuffer: ArrayBuffer[String] = new ArrayBuffer[String]()

        strBuffer.append(tokens.get(i))
        strBuffer.append(posTags.get(i))
        if (governors.get(i).isPresent && governors.get(i).get() != -1)
          strBuffer.append(tokens.get(governors.get(i).get()))
        else
          strBuffer.append("NULL")
        if (dependencies.get(i).isPresent)
          strBuffer.append(dependencies.get(i).get())
        else
          strBuffer.append("NULL")

        var ioTag = "O"
        if (aspectTerm.contains(tokens.get(i)))
          ioTag = "I"

        tokenBuffer.append(Token.put(ioTag, strBuffer.toArray))
      }
      (item._1, new Sequence(tokenBuffer.toArray))
    }).map(_._2)

    featureVectorRdd.saveAsTextFile("Feature Vector")

    val splits: Array[RDD[Sequence]] = featureVectorRdd.randomSplit(Array(0.7, 0.3))

    val trainRdd: RDD[Sequence] = splits(0)

    val testRdd: RDD[Sequence] = splits(1)

    val gsAspectRdd: RDD[String] = testRdd
      .map(sequence => {
        sequence.toArray
          .filter(token => token.label.equals("I"))
          .map(token => token.tags.apply(0))
      }).map(_.mkString("\n"))
      .filter(_.nonEmpty)
      .distinct()

    gsAspectRdd.saveAsTextFile("GSAspect")

    val templates: Array[String] = scala.io.Source.fromFile("/Users/hardikdalal/github/ate-crf-spark/template").getLines().filter(_.nonEmpty).toArray
    val model: CRFModel = CRF.train(templates, trainRdd, 0.25, 1, 100, 1E-3, "L1")

    val resultRdd: RDD[Sequence] = model.predict(testRdd)

    val extractedAspectRdd: RDD[String] = resultRdd
      .map(sequence => {
        sequence.toArray
          .filter(token => token.label.equals("I"))
          .map(token => token.tags.apply(0))
      }).map(_.mkString("\n"))
      .filter(_.nonEmpty)
      .distinct()

    extractedAspectRdd.saveAsTextFile("ExtractedAspects")

    val score = gsAspectRdd.intersection(extractedAspectRdd).collect().length
    val precision = score / extractedAspectRdd.count()
    val recall = score / gsAspectRdd.count()

    println("Precision = " + precision)
    println("Recall = " + recall)

    sc.stop()
  }

}
