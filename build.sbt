name := "RAT_AWS"

version := "1.0"

sparkVersion := "2.0.0"

sparkComponents += "mllib"

resolvers += Resolver.sonatypeRepo("public")

spDependencies += "databricks/spark-corenlp:0.2.0-s_2.11"

libraryDependencies += "org.scalanlp" %% "breeze" % "0.8.1"
