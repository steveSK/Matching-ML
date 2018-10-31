package matching.ml.datamining.worldcheck

import java.io.{File, PrintWriter}
import java.net.FileNameMap

import matching.lucene.analyzers.{NgramAnalyzer, SkipGramAnalyzer}
import matching.lucene.differences.{CompressedLengtDifference, ConsonantsDifference, VowelsDifference}
import matching.lucene.distances._
import org.apache.commons.codec.language.DoubleMetaphone
import org.apache.lucene.search.spell.{JaroWinklerDistance, LevensteinDistance, StringDistance}
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.joda.time.{DateTime, DateTimeZone}
import FeatureGeneratorDistances._
import matching.ml.sparkapi.SparkService

import scala.io.Source

/**
  * Created by stefan on 12/15/16.
  */
/***
  * case class for machine learning string matching containing pair of string and there attributes, for world check
  *
  *
  * @param string1 first string of pair
  * @param string2 second string of pair
  * @param uid - uid in world check
  * @param personType person type (natural or legal)
  * @param citizenship citizenship of person (for blocking)
  * @param fullNameNgramDistance ngram distance based on combination of first and last name
  * @param fullNameJaroWinklerDistance Jaro-Winkler distance based on combination of first and last name
  * @param fullNameDoubleMetaphoneDistance full-name distance based on combination of first and last name
  * @param label - binary label
  */
case class FeaturedRecord(string1: String, string2: String,uid: String, personType: String, citizenship: String, fullNameNgramDistance: Double,
                          fullNameJaroWinklerDistance: Double, fullNameDoubleMetaphoneDistance: Double, label: Option[Double] = None) {
  override def toString: String = {
    return (string1 + ";" + string2 + ";" + uid + ";" + personType + ";" + citizenship + ";"
      + fullNameNgramDistance + ";" + fullNameJaroWinklerDistance + ";" + fullNameDoubleMetaphoneDistance  + ";" +  label.get)
  }
}

/***
  * case class for already matched record
  * @param uid uid in world-check database
  * @param name - full name of the person
  * @param citizenship - citizenship of person
  * @param personType person type
  */
case class MatchRecord(uid: String,name: String,citizenship: String,personType: String)

/***
  * singleton class for generating distances
  */
object FeatureGeneratorDistances {

  val fullNameDelimeter = ","

  val fullNameNgramDistance = new FullNameSimilarity(new NGramDistance(new NgramAnalyzer(2, 3),
    new NgramAnalyzer(2, 2)),
    new NGramDistance(new NgramAnalyzer(2, 3), new NgramAnalyzer(2, 2)), 0.3, 0.7)
  val fullNameJaroWinklerDistance = new FullNameSimilarity(new JaroWinklerStringDistance, new JaroWinklerStringDistance, 0.3, 0.7)
  val fullNameDoubleMetaphoneDistance = new FullNameSimilarity(new DoubleMetaphoneDistance, new DoubleMetaphoneDistance, 0.3, 0.7)

  /***
    * generate feauture distances for person against name
    * @param s1 name to match
    * @param s2 person name
    * @param uid uid of person
    * @param personType  person type
    * @param citizenship citizenship of person
    * @param label label
    * @return featured record
    */
  def generateFeaturesFromPair(s1: String, s2: String,uid: String, personType: String, citizenship: String, label: Option[Double] = None): FeaturedRecord = {
    val firstName1 = s1.split(fullNameDelimeter)(1)
    val firstName2 = s2.split(fullNameDelimeter)(1)

    val lastName1 = s1.split(fullNameDelimeter)(0)
    val lastName2 = s2.split(fullNameDelimeter)(0)

    FeaturedRecord(s1, s2,uid,personType,citizenship,
      fullNameNgramDistance.getDistance(firstName1, firstName2, lastName1, lastName2),
      fullNameJaroWinklerDistance.getDistance(firstName1, firstName2, lastName1, lastName2),
      fullNameDoubleMetaphoneDistance.getDistance(firstName1, firstName2, lastName1, lastName2),label)
  }

}

/***
  * Class to generate features for matching
  */
object FeatureGenerator extends Serializable {

  import SparkService.sparkSession.implicits._

  private val LABEL_COLUMN_NAME = "label"

  /***
    * generate dataset of features from labeled dataset containing pair of string for training the ML model
    *
    * @param labeledDataSetFile - input file containing labeled pairs
    * @param columnNamesString - seq names for columns
    * @return dataset with features
    */
  def generateTrainingFeatureDatasetFromFile(labeledDataSetFile: String, columnNamesString: Seq[String]): Dataset[Row] = {
    val input = SparkService.loadDataFromCSV(labeledDataSetFile, ":",true).filter(x => x.getString(0) != null && x.getString(1) != null && x.getString(2) != null)
    val inputFileDF = input.filter(x => x.getString(0).split(",").size > 1 && x.getString(1).split(",").size > 1)

    inputFileDF.map(x => generateFeaturesFromPair(x.getString(0), x.getString(1),"","","", Some(x.getString(2).trim match {
      case "T" => 1.0
      case "F" => 0.0
    }))).toDF(columnNamesString: _*)
  }


  /***
    * generate features by matching a single name against file of persons
    *
    * @param s1 - person name to match
    * @param citizenship citizenship for blocking
    * @param inputFileName input file containing person list
    * @param delimeter delimeter of input file
    * @return dataset of pairs ready for matching
    */
  def generateFeatureForMatchAgainstFile(s1: String, citizenship: String, inputFileName: String, delimeter: String): Dataset[Row] = {
    import SparkService.sparkSession.implicits._
    val inputFileDF = SparkService.loadDataFromCSV(inputFileName, "|",true)
    val blockedDF = inputFileDF.filter($"citizenship" === citizenship)
    val inputFileDFNaturalFlatted = blockedDF.flatMap{
      case x => {
        x.getString(1).split(";").map(MatchRecord(x.getInt(0).toString,_,x.getString(2),x.getString(3)))
        x.getString(4).split(";").map(MatchRecord(x.getInt(0).toString,_,x.getString(2),x.getString(3)))
        x.getString(5).split(";").map(MatchRecord(x.getInt(0).toString,_,x.getString(2),x.getString(3)))
        x.getString(6).split(";").map(MatchRecord(x.getInt(0).toString,_,x.getString(2),x.getString(3)))
      }
    }

    val inputFileDFFiltered = inputFileDFNaturalFlatted
      .filter(x => x.name != null && x.name != "null"
        && x.name.nonEmpty).filter(x => x.name.split(",").size > 1)

    val recordCount = inputFileDFFiltered.count()
    println("Number of strings to match:" + recordCount)
    val startTime = DateTime.now(DateTimeZone.UTC).getSecondOfDay
    val featuredDS = inputFileDFFiltered.map(x => generateFeaturesFromPair(s1, x.name,x.uid,x.personType,x.citizenship)).toDF().drop(LABEL_COLUMN_NAME)

    val finishTime = DateTime.now(DateTimeZone.UTC).getSecondOfDay
    println("Finished in: " + (finishTime - startTime) + "s")
    println("Processing speed: " + recordCount.toDouble / (finishTime - startTime).toDouble + "records/s")
    featuredDS
  }

}
