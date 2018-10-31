package matching.ml.datamining.worldcheck

import java.io.{File, PrintWriter}
import java.text.Normalizer

import matching.lucene.analyzers.NgramAnalyzer
import matching.lucene.distances.NGramDistance
import matching.lucene.utils.LuceneUtils
import matching.ml.sparkapi.SparkService
import org.apache.lucene.analysis.Analyzer
import org.apache.spark.sql.functions.udf
import org.apache.lucene.search.spell.JaroWinklerDistance

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

/**
  * Class helper to able to parse and generate data from World Check database
  * Created by stefan on 12/9/16.
  */
class WorldCheckAliases(analyzer: Analyzer) {
  private val delimeter = '|'
  private val cleanDelimeter = """\|"""
  private val citizenshipColumn = "citizenship"
  private val personTypeColum = "e/i"
  private val stringDistance = new NGramDistance(new NgramAnalyzer(2, 3), new NgramAnalyzer(2, 2))
  private val similarityRanges = Array(0.0, 0.3, 0.7,1.0)


  /***
    * generate false matches from aliases file, by combining aliases of different persons using scala random methods.
    * to make final dataset more representative, final dataset contain matches diferent similarities from ranges defined
    * in parameter similarityRanges. Final result contains equal number of false matches from every range
    * Result is written to output file
    * @param aliasesNamesFile input file containing aliases
    * @param outputFile output file
    * @param sampleSize size of sample from which matches are taken
    * @param numberOfPairs number of matches to generate
    */
  def generateSimilarFalseMatches(aliasesNamesFile: String, outputFile: String, sampleSize: Int, numberOfPairs: Int) = {
    val input = Source.fromFile(aliasesNamesFile).getLines().map(x => x.split(delimeter).toList).toList
    val sampledInput = scala.util.Random.shuffle(input).take(sampleSize)
    var finalResult : List[String] = List()
    var count = 0
    val filtered = sampledInput.map(x => x.filter(s => s!= null && s.split(" ").length > 1)).filter(x => x.size!=0)
    for(i <- 1 until  similarityRanges.length) {
       finalResult = finalResult ++ getFalsePairs(filtered,i,numberOfPairs/(similarityRanges.length -1))
      }
    writeResults(finalResult, outputFile, Some("False Positives:"))
  }
  // method to generate matches from given range
  private def getFalsePairs(sample : List[List[String]],i: Int,numberOfPairs: Int) : List[String] = {
    val result : ListBuffer[String] = ListBuffer()
    var count = 0
    val random = new Random()
    while(true){
      val nested = sample(random.nextInt(sample.size))
      val filtered = sample.filter(_ != nested)
      val x = filtered(random.nextInt(filtered.size))
      val s1 = nested(random.nextInt(nested.size))
      val s2 = x(random.nextInt(x.size))
      if(inSimilarityRange(s1,s2,similarityRanges(i-1),similarityRanges(i)) && !result.contains(s2 + " : " + s1)){
        println("found: " + s1 + " : " + s2 + " ratio(" + similarityRanges(i-1)+ "," + similarityRanges(i) + ") count: " + count)
        result+= s1 + " : " + s2
        count +=1
      }
      if(count == numberOfPairs){
        return result.toList
      }

    }
    result.toList
  }

  /***
    * generate true matches by combining aliases of same person
    *
    * Result is written to output file
    * @param aliasesNamesFile input file with aliases
    * @param outputFile output file
    * @param minimalSimilarity  minimal similarity to be match considered at true match
    */
  def generateSimilarTrueMatches(aliasesNamesFile: String, outputFile: String ,minimalSimilarity : Double) = {
    val input = Source.fromFile(aliasesNamesFile).getLines().map(x => x.split(delimeter).toList).toList
    val filtered = input.map(x => x.filter(s => s!= null && s.split(" ").size >1))
    val results = filtered.flatMap(x => x.map(y => x.drop(x.indexOf(y) + 1).
      filter(y1 => (stringDistance.getDistance(y, y1) >= minimalSimilarity  && !y.equals(y1))).map(y1 => y + " : " + y1)).flatten)
    println(results.size)
    writeResults(results, outputFile, Some("True Positives:"))
  }


  /***
    *  decide if similarity of pair of strings lay in given range or not
    *
    * @param s1   string 1
    * @param s2   string 2
    * @param lowerTreshold   lower range bound
    * @param upperTreshold   upper range bound
    * @return true if similarity is in given range, false otherwise
    */
  private def inSimilarityRange(s1: String, s2: String, lowerTreshold: Double, upperTreshold: Double) : Boolean = {
    val stringDiff = stringDistance.getDistance(LuceneUtils.removeSpecialCharecters(s1), LuceneUtils.removeSpecialCharecters(s2))
    stringDiff >= lowerTreshold && stringDiff <= upperTreshold
  }

  /***
    * generate first name frequency list from given file
    *
    * @param inputFile input file
    * @param nameFreqList output freq list file
    */
  def generateNameFrequencyList(inputFile: String, nameFreqList: String) : Unit = {
    val input = Source.fromFile(inputFile).getLines().map(x => x.split(delimeter).toList).toList
    println(input.length)

    val listNorm = input.map(x => x.map(y => y.replace(",", " ").split(" ").map(z => z.toLowerCase)).flatten).flatten.filter(x => x.nonEmpty)
    println(listNorm.size)
    val result = listNorm.groupBy(word => word).mapValues(_.size).toList.sortBy(x => x._2).reverse;
    println(result.size)
    val file = new File(nameFreqList)

    val writer = new PrintWriter(file)
    result.foreach(x => writer.write(x._1 + "," + x._2 + "\n"))
  }

  /***
    * merge True and False matches to one file and generate labels
    *
    * @param inputFPFile input false matches file
    * @param inputTPFile input true matches file
    * @param outputFile output file
    * @param balanceDataset
    */
  def mergeTFMatchesFromDir(inputFPFile: String, inputTPFile: String, outputFile: String, balanceDataset: Boolean): Unit ={
    val falsePositives =  Source.fromFile(inputFPFile).getLines().map(x => x + ":F").toList;
    val truePositives =   Source.fromFile(inputTPFile).getLines().map(x => x + ":T").toList;

    val result = balanceDataset match {
      case true => {
        val smaller = if(falsePositives.size < truePositives.size) falsePositives else truePositives
        val larger =  if(falsePositives.size > truePositives.size) falsePositives else truePositives
        Random.shuffle(smaller ::: Random.shuffle(larger).take(smaller.size))
      }
      case _ => Random.shuffle(falsePositives ::: truePositives)
    }



    val file = new File(outputFile)
    val writer = new PrintWriter(file)
    result.foreach(x => writer.write(x + "\n"))
  }

  /***
    * generate feature dataset from labeled dataset to file
    *
    * @param labeledPairsFile input labeled dataset
    * @param outputFile output file
    * @param columns columns which will be in final dataset
    */
  def generateFeaturesFromLabeledPairs(labeledPairsFile: String, outputFile: String, columns: Seq[String]): Unit = {

      val resultDF = FeatureGenerator.generateTrainingFeatureDatasetFromFile(labeledPairsFile,columns)
      val resultToPrint = resultDF.columns.mkString(";") :: resultDF.collect.toList.map(x => x.mkString(";"))
      writeResults(resultToPrint,outputFile)
  }

  /***
    * merge true and false matches from dir
    *
    * @param filesDirPath dir where matches are stored
    * @param outputFile output file
    */
  def mergeTFMatchesFromDir(filesDirPath: String, outputFile: String): Unit = {
    val d = new File(filesDirPath)
    if (d.exists && d.isDirectory) {
      val fileList = d.listFiles.filter(_.isFile).toList
      val truePositives =  fileList.filter(x => x.getName.contains("true"))
        .flatMap(x =>  Source.fromFile(x).getLines().map(x => x + " :T").toList)
      val falsePositives =  fileList.filter(x => x.getName.contains("false"))
        .flatMap(x =>  Source.fromFile(x).getLines().map(x => x + " :F").toList)
      writeResults(Random.shuffle(truePositives++falsePositives),outputFile)
    } else {
      throw new IllegalArgumentException("path is not a directory")
    }
  }


  /***
    * calculate citizenship distribution over final dataset
    *
    * @param inputFile input file
    * @param outputFile output file
    */
  def generateCitizenshipStatistic(inputFile: String, outputFile: String): Unit = {
    println("Generating Citizenship statistic: ")
    val sparkSession = SparkService.sparkSession
    import sparkSession.implicits._

    val inputDF =  SparkService.loadDataFromCSV(inputFile,"|",true);
    val groupedByCitizenshipDF = inputDF.select(citizenshipColumn).groupBy(citizenshipColumn).count().sort($"count".desc);
    groupedByCitizenshipDF.show()
    val resultToPrint = groupedByCitizenshipDF.collect.toList.map(x => x.getString(0) + ": " + x.getLong(1).toString)
    writeResults(resultToPrint,outputFile,Some("CITIZENSHIP COUNT:"))
  }

  /***
    * filter records by citizenships
    *
    * @param citizenships citizenships to filter
    * @param inputFile input file
    * @param outputFile output file
    *
    */
  def filterByCitizenship(citizenships: List[String],inputFile: String, outputFile: String): Unit = {
    val sparkSession = SparkService.sparkSession
    val inputDF =  SparkService.loadDataFromCSV(inputFile,"|",true)
    val citizenshipIndex = inputDF.columns.indexOf(citizenshipColumn)
    val filteredDF = inputDF.filter(x => citizenships.contains(x.getString(citizenshipIndex)))
    val resultToPrint = filteredDF.collect.toList.map(x => x.mkString("|"));
    writeResults(filteredDF.columns.mkString("|") :: resultToPrint,outputFile)
  }

  /***
    * filter natural names from file
    *
    *
    * @param inputFile input file to filter
    * @param outputFile output file
    */
  def filterNaturalNames(inputFile: String, outputFile: String): Unit = {
    val sparkSession = SparkService.sparkSession
    import sparkSession.implicits._
    val inputDF =  SparkService.loadDataFromCSV(inputFile,"|",true);
    val inputPersonsDF =  inputDF.filter($"e/i" !== "e").drop(personTypeColum)
    val resultToPrint = inputPersonsDF.columns.mkString("|") :: inputPersonsDF.collect.toList.map(x => x.mkString("|"));
    writeResults(resultToPrint,outputFile)
  }

  /***
    * remove short aliases from file, allowing only aliases with first and last name defined
    *
    * @param inputFile input file
    * @param outputFile output file
    */
  def removeShortAliases(inputFile: String, outputFile: String): Unit = {
    val sparkSession = SparkService.sparkSession
    import sparkSession.implicits._
    val inputDF =  SparkService.loadDataFromCSV(inputFile,"|",true);
    val func: String => String = {
      _.split(";").filter(x => x.split(",").length > 1).mkString(";")
    }
    val funcUDF = udf(func)
    val filteredDF = inputDF.withColumn("upd_aliases",funcUDF($"aliases")).drop("aliases").withColumnRenamed("upd_aliases","aliases")
                            .withColumn("upd_low quality aliases",funcUDF($"low quality aliases")).drop("low quality aliases").withColumnRenamed("upd_low quality aliases","low quality aliases")
                            .withColumn("upd_alternative spelling",funcUDF($"alternative spelling")).drop("alternative spelling").withColumnRenamed("upd_alternative spelling","alternative spelling")
    val resultToPrint = filteredDF .columns.mkString("|") :: filteredDF.collect.toList.map(x => x.mkString("|"));
    writeResults(resultToPrint,outputFile)

  }

  /***
    * clean data from noise
    *
    * @param inputFile file path to clean
    * @param outputFile output file path to print
    */
  def cleanData(inputFile: String, outputFile: String): Unit = {
    println("cleaning data from file: " + inputFile)
    val input = Source.fromFile(inputFile,"ISO-8859-1").getLines().map(x => x.split(cleanDelimeter).toList).toList
    val output = input.map(x => x.map(y => cleanRecord(y))).map(x => x.mkString(cleanDelimeter))
    writeResults(output, outputFile)

  }


  /***
    * normalize record to contains just ASCII characters
    *
    * @param string string to clean
    * @return cleaned string
    */
  def cleanRecord(string: String): String = {
    Normalizer.normalize(string, Normalizer.Form.NFD).toLowerCase.replaceAll("[^\\p{ASCII}]", "").replace("-", "")
  }

  /***
    * remove stopwords from file
    *
    * @param inputFile input file
    * @param outputFile output file
    * @param stopwordsFile file with stopwords
    */
  def removeStopWords(inputFile: String, outputFile: String,stopwordsFile: String): Unit = {
    val stopwords = Source.fromFile(stopwordsFile).getLines().toList
    val input = Source.fromFile(outputFile).getLines().map(x => x.split(cleanDelimeter).toList).toList
    val output = input.map(x => x.map(y => cleanRecord(y))).map(x => x.mkString(cleanDelimeter))
    writeResults(output, outputFile)
  }



  private def delStopWords(s: String, stopwords: List[String]): String ={
    val split = s.split(" ")
    val result = split.filter(x => containsSW(x,stopwords)).mkString(" ")
    result
  }

  private def containsSW(s: String, stopwords: List[String]) : Boolean = {
    val shortWords = List("di","ou,","for","de","in","der", "vyi", "gen", "capo", "i", "del")
    for(word <-stopwords){
      if(s.contains(word.trim) || shortWords.contains(s)){
        return false;
      }
    }
    return true
  }



  private def writeResults(results: List[String], outputFile: String, name: Option[String] = None) {
    val file = new File(outputFile)
    val writer = new PrintWriter(file,"UTF-8")
    if(name.isDefined) {
      writer.write(name.get.replaceAll("[^\\x20-\\x7e]", "") + "\n")
    }
    results.foreach(x => writer.write(x + "\n"))
    writer.close()
  }

}
