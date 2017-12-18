name := "ATE-CRF-SPARK"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("com.intel" % "imllib_2.11" % "0.0.1",
  "databricks" % "spark-corenlp" % "0.2.0-s_2.11",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" classifier "models",
  "edu.stanford.nlp" % "stanford-parser" % "3.6.0")