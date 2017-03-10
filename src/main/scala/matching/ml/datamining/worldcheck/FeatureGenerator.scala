package matching.ml.datamining.worldcheck

import java.io.{File, PrintWriter}

import matching.lucene.analyzers.{NgramAnalyzer, SkipGramAnalyzer}
import matching.lucene.differences.{CompressedLengtDifference, ConsonantsDifference, VowelsDifference}
import matching.lucene.distances.{DoubleMetaphoneDistance, LCSDistance, NGramDistance, NameFrequencyDistance}
import org.apache.commons.codec.language.DoubleMetaphone
import org.apache.lucene.search.spell.{JaroWinklerDistance, LevensteinDistance}

import scala.io.Source

/**
  * Created by stefan on 12/15/16.
  */

case class FeaturedRecord(string1: String, string2: String, ngramDistance: Double,
                          jaroWinklerDistance: Double, levensteinDistance: Double, doubleMetaphoneDistance: Double,
                          lcsDistance: Double, nameFrequencyDistance: Double, vowelsDifference: Int,
                          consonantsDifference: Int, stringLengthDifference: Int, label: Double) {
  override def toString: String = {
        return (string1 + ";" + string2 + ";" + ngramDistance + ";" + jaroWinklerDistance + ";" + levensteinDistance + ";"  +
          doubleMetaphoneDistance + ";" + lcsDistance + ";" + nameFrequencyDistance + ";" + vowelsDifference + ";"  +
          consonantsDifference + ";" + stringLengthDifference + ";" + label)
  }
}

class FeatureGenerator(labeledDataSetFile: String) {

  val labeledDataSet = Source.fromFile(labeledDataSetFile).getLines()


  def generateFeatures(outputFile: String, freqFile: String): Unit = {
    val nGramDistance = new NGramDistance(new SkipGramAnalyzer(1, 3), new NgramAnalyzer(2, 2))
    val jaroWinklerDistance = new JaroWinklerDistance
    val levensteinDistance = new LevensteinDistance
    val doubleMetaphoneDistance = new DoubleMetaphoneDistance
    val lCSDistance = new LCSDistance
    val nameFrequencyDistance = new NameFrequencyDistance(freqFile)
    val vowelsDifference = new VowelsDifference
    val consonantsDifference = new ConsonantsDifference
    val stringLengthDifference = new CompressedLengtDifference


    val file = new File(outputFile)
    val writer = new PrintWriter(file)
    for (record <- labeledDataSet) {
      println(record)
      val split = record.split(':')
      val s1 = split(0)
      val s2 = split(1)
      val label = split(2) match {
        case "T" => 1.0
        case "F" => 0.0
      }
      val featuredRecord = new FeaturedRecord(s1, s2, nGramDistance.getDistance(s1, s2), jaroWinklerDistance.getDistance(s1, s2),
        levensteinDistance.getDistance(s1, s2), doubleMetaphoneDistance.getDistance(s1, s2), lCSDistance.getDistance(s1, s2),
        nameFrequencyDistance.getDistance(s1, s2), vowelsDifference.getDifferece(s1, s2), consonantsDifference.getDifferece(s1, s2),
        stringLengthDifference.getDiffrence(s1, s2), label)
      writer.write(featuredRecord.toString + "\n")
    }


    writer.close()
  }


}
