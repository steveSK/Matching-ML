package matching.ml.datamining.worldcheck

import java.io.{File, PrintWriter}

import matching.lucene.utils.LuceneUtils
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.search.spell.JaroWinklerDistance

import scala.io.Source
import scala.util.Random

/**
  * Created by stefan on 12/9/16.
  */
class WorldCheckAliases(analyzer: Analyzer, aliasesNamesFile: String, outputFPFile: String, outputTPFile: String, similarityTreshold: Double) {
  private val minimalSimilarityforTP = 0.4
  private val delimeter = ";";
 // private val stringDistance = new NGramDistance(analyzer, new NgramAnalyzer(2, 2))
  private val stringDistance = new JaroWinklerDistance();
  private val input = Source.fromFile(aliasesNamesFile).getLines().map(x => x.split(delimeter).toList).toList


  def generateSimilarFalseMatches = {
    val results = input.map(x => x.filter(s => (s.split(" ").length > 1))).map(x => x.map(y => input.drop(input.indexOf(x)).flatten
      .filter(y1 => stringDistance.getDistance(LuceneUtils.removeSpecialCharecters(y),
        LuceneUtils.removeSpecialCharecters(y1)) > similarityTreshold).map(y1 => y + " : " + y1)).flatten).flatten
    writeResults(results, outputFPFile, "False Positives:")
  }

  def generateSimilarTrueMatches = {
    val results = input.flatMap(x => x.filter(s => (s.split(" ").length > 1)).map(y => x.drop(x.indexOf(y + 1)).
      filter(y1 => (stringDistance.getDistance(y, y1) > minimalSimilarityforTP && !y.equals(y1))).flatMap(y1 => y + " :" + y1).toString()))
    writeResults(results, outputTPFile, "True Positives:")
  }


  def generateNameFrequencyList(inputFile: String, nameFreqList: String) : Unit = {
    val input = Source.fromFile(inputFile).getLines().map(x => x.split(delimeter).toList).toList
    println(input.length)
   // val inputFileSplitted = input.map(x => x.map(y => y.split(" ")).flatten).flatten

    val listNorm = input.map(x => x.map(y => y.replace(",", " ").split(" ").map(z => z.toLowerCase)).flatten).flatten.filter(x => x.nonEmpty)
    println(listNorm.size)
    val result = listNorm.groupBy(word => word).mapValues(_.size).toList.sortBy(x => x._2).reverse;
    println(result.size)
    val file = new File(nameFreqList)

    val writer = new PrintWriter(file)
    result.foreach(x => writer.write(x._1 + "," + x._2 + "\n"))
  }


  def generateLabels(outputFile: String): Unit ={
    val falsePositives =  Source.fromFile(outputFPFile).getLines().map(x => x + ":F").toList;
    val truePositives =   Source.fromFile(outputTPFile).getLines().map(x => x + ":T").toList;

    val result = Random.shuffle(falsePositives ::: truePositives);

    val file = new File(outputFile)
    val writer = new PrintWriter(file)
    result.foreach(x => writer.write(x + "\n"))
  }

  def generateBorderCases(inputFile: String, outputFile: String): Unit ={
    val input =  Source.fromFile(inputFile).getLines()
    val filteredInput = input.filter(x => {
      val distance = stringDistance.getDistance(x.split(":")(0), x.split(":")(1))
      distance > 0.3 && distance < 0.5
    })
    writeResults(filteredInput.toList,outputFile,"Border Cases:")

  }


  private def writeResults(results: List[String], outputFile: String, name: String) {
    val file = new File(outputFile)
    val writer = new PrintWriter(file)
    writer.write(name + "\n")
    results.foreach(x => writer.write(x + "\n"))
    writer.close()
  }

}
