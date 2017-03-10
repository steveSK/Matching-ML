name := "matching-ml"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "Restlet Repository" at "http://maven.restlet.org"

libraryDependencies += "org.apache.lucene" % "lucene-analyzers-common" % "5.3.1"
libraryDependencies += "org.apache.lucene" % "lucene-core" % "5.3.1"
libraryDependencies += "org.apache.lucene" % "lucene-queryparser" % "5.3.1"
libraryDependencies += "org.apache.solr" % "solr-solrj" % "5.3.1"
libraryDependencies += "org.apache.solr" % "solr-core" % "5.3.1"
libraryDependencies += "org.restlet.jee" % "org.restlet.ext.servlet" % "2.3.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.1"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.0.1"
libraryDependencies += "nz.ac.waikato.cms.weka" % "weka-stable" % "3.6.6"
