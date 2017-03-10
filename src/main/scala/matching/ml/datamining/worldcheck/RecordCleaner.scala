package matching.ml.datamining.worldcheck

import java.io.{File, PrintWriter}
import java.text.Normalizer

import scala.io.Source

/**
  * Created by stefan on 12/12/16.
  */
class RecordCleaner(inputFile: String, outputFile: String, stopwordsFile: String) {

  val delimeter = ";"
  var i = 0
  val shortWords = List("di","ou,","for","de","in","der", "vyi", "gen", "capo", "i", "del")

  def cleanData(): Unit = {
    val input = Source.fromFile(inputFile).getLines().map(x => x.split(delimeter).toList).toList
    val output = input.map(x => x.map(y => cleanRecord(y)))
    writeResults(output, outputFile)

  }


  private def writeResults(results: List[List[String]], outputFile: String) {
    val file = new File(outputFile)
    val writer = new PrintWriter(file)
    results.foreach(x => {
      x.foreach(y => writer.write(y + ";"))
      writer.write("\n")
    })
    writer.close()
  }


  def cleanRecord(string: String): String = {
    Normalizer.normalize(string, Normalizer.Form.NFD).toLowerCase.replaceAll("[^\\p{ASCII}]", "").replace("-", "")
  }

  def removeStopWords(): Unit = {
    val stopwords = Source.fromFile(stopwordsFile).getLines().toList
    val input = Source.fromFile(outputFile).getLines().map(x => x.split(delimeter).toList).toList

    val output = input.map(x => x.map(y => delStopWords(y,stopwords).trim))
    writeResults(output, outputFile)
  }

  private def delStopWords(s: String, stopwords: List[String]): String ={
    val split = s.split(" ")
    val result = split.filter(x => containsSW(x,stopwords)).mkString(" ")
    result
  }

  private def containsSW(s: String, stopwords: List[String]) : Boolean = {
    for(word <-stopwords){
    if(s.contains(word.trim) || shortWords.contains(s)){
      return false;
    }
  }
    return true
  }


}
