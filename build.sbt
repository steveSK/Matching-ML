name := "matching-ml"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "Restlet Repository" at "http://maven.restlet.org"
resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

libraryDependencies += "org.apache.lucene" % "lucene-analyzers-common" % "5.3.1"
libraryDependencies += "org.apache.lucene" % "lucene-core" % "5.3.1"
libraryDependencies += "org.apache.lucene" % "lucene-queryparser" % "5.3.1"
libraryDependencies += "org.apache.solr" % "solr-solrj" % "5.3.1"
libraryDependencies += "org.apache.solr" % "solr-core" % "5.3.1"
libraryDependencies += "org.restlet.jee" % "org.restlet.ext.servlet" % "2.3.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.1.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.2.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "com.github.scala-incubator.io" % "scala-io-file_2.11" % "0.4.3"


assemblyMergeStrategy in assembly := {
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("aopalliance", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/DUMMY.SF" => MergeStrategy.discard
  case "META-INF/DUMMY.DSA" => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.last
  case x => MergeStrategy.last

}





