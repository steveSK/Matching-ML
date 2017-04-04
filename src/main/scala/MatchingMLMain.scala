package matching

import java.io.{File, FileInputStream, PrintWriter}
import java.util.Properties

import matching.lucene.analyzers.SkipGramAnalyzerWithTokenizer
import matching.ml.spark.{AlgorithmEvaluator, SparkService}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.io.Source

/**
  * Created by stefan on 12/9/16.
  */
object MatchingMLMain {
  private val analyzer = new SkipGramAnalyzerWithTokenizer(1, 3)
  private val prop = new Properties()
  private val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "192.158.0.145:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "0",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean),
    "request.timeout.ms" -> (40000: java.lang.Integer),
    "heartbeat.interval.ms" -> (3000: java.lang.Integer),
    "auto.commit.interval.ms" -> (500: java.lang.Integer)
  )
//  private val resultfile = "~/test-result"
  /*  private val inputFile = "/media/stefan/D27C48117C47EEB3/matching-data/aliases-set"
    private val cleanedInputFile = "/media/stefan/D27C48117C47EEB3/matching-data/aliases-set-cleaned"
    private val outputFPFile = "/media/stefan/D27C48117C47EEB3/matching-data/false-aliases-set"
    private val outputTPFile = "/media/stefan/D27C48117C47EEB3/matching-data/true-aliases-set"
    private val outputBorderCases = "/media/stefan/D27C48117C47EEB3/matching-data/border-cases"
    private val labeledOutputFile = "/media/stefan/D27C48117C47EEB3/matching-data/labeled-aliases-set"
    private val stopWordsFile = "/media/stefan/D27C48117C47EEB3/matching-data/stopwords"
    private val featuredDataSetFile = "/media/stefan/D27C48117C47EEB3//matching-data/dataset-with-features"
    private val names = "/media/stefan/D27C48117C47EEB3/matching-data/just-names"
    private val frequencyNameFile = "/media/stefan/D27C48117C47EEB3/matching-data/name-frequencies"
    private val similarityRatio: Double = 0.8
    private val numberOfFeautures = 9 */


  def main(args: Array[String]) {
    System.out.println("Start with mining: ")
    val propertyFile = args(0)
    prop.load(new FileInputStream(propertyFile))
    val inputFile = prop.getProperty("input.file.set")
    val cleanedInputFile = prop.getProperty("input.file.cleaned")
    val outputFPFile = prop.getProperty("output.file.fp")
    val outputTPFile = prop.getProperty("output.file.tp")
    val outputBorderCases = prop.getProperty("output.file.bordercases")
    val labeledOutputFile = prop.getProperty("output.file.labeled")
    val stopWordsFile = prop.getProperty("input.stopwords")
    val featuredDataSetFile = prop.getProperty("output.file.feature.dataset")
    val names = prop.getProperty("input.file.names")
    val frequencyNameFile = prop.getProperty("output.file.frequencies")
    val similarityRatio = prop.getProperty("similarity.ratio").toDouble
    val numberOfFeautures = prop.getProperty("features.number").toInt


    //   val cleaner = new matching.ml.datamining.worldcheck.RecordCleaner(inputFile,cleanedInputFile,stopWordsFile)
    //    cleaner.cleanData()
    //    cleaner.removeStopWords()
    //   val wch = new WorldCheckAliases(analyzer, cleanedInputFile, outputFPFile, outputTPFile, similarityRatio)
    // wch.generateNameFrequencyList(names, frequencyNameFile)
    //   wch.generateBorderCases(outputTPFile,outputBorderCases)

    //  wch.generateSimilarFalseMatches
    //  wch.generateSimilarTrueMatches
    //   wch.generateLabels(labeledOutputFile)
    //  val fg = new FeatureGenerator(labeledOutputFile)
    //   fg.generateFeatures(featuredDataSetFile,frequencyNameFile)

    val service = new SparkService(null)
    val evaluator = new AlgorithmEvaluator(service)
    val ignored = List("string1", "string2", "label")
    val featuresNames = firstLine(new File(featuredDataSetFile.replace("file://","").replace("file:",""))).get.split(";").toList.filter(x => !ignored.contains(x))
   // val datasets = service.getTrainAndTestDataFromFileML(featuredDataSetFile, Array(0.9, 0.1), 42, ignored)
    val weights = Array(0.9, 0.1)
    val seed = 42
    val topic = "test"
    val offsets = Array((0,460095))
    val labelName = "offset"
    val datasets = service.getTrainAndTestDataFromKafka(topic,offsets,kafkaParams,weights,seed,ignored,labelName)
    val fullDataset = datasets._1 ++ datasets._2







    //cros-validation setup
    //   val pipeline = new Pipeline().setStages(Array(service.buildRandomForest()))
    //   val paramGrid = new ParamGridBuilder().build()
    //   val cvModel = service.crossValidate(fullDataset.toDF("label","features"),paramGrid,pipeline,3)
    //   val RFresult2 = service.evaluateTestDataML(datasets._2,cvModel)


    val writer = new PrintWriter(prop.getProperty("output.file.result"))
    // val RFResult1 = evaluator.evaluateAlgorithmSetup(datasets,Array(service.buildRandomForest()))
    //   evaluator.evaluateFeature(datasets,Array(service.buildRandomForest()),1)


    // writer.write("Result1 is:" + NBresult + "\n")
    //  println("Result1 is:" + NBresult)
    //   writer.write("Random Forest is:" + RFResult1 + "\n")
    //  println("Random Forest is:" + RFResult1)
    // writer.write("Random Forest after CV is:" + RFresult2 + "\n")
    //  println("Random Forest after CV is:" + RFresult2)
    //   writer.write("Result3 is:" + LRresult + "\n")
    //  println("Resul3 is:" + LRresult)
//    println("evaluate particular feautures: ")
//    writer.write("evaluate particular feautures:\n")
    val numClasses = prop.getProperty("randomforest.classes.number").toInt
    val numTrees = prop.getProperty("randomforest.trees.number").toInt
    val featureSubsetStrategy = prop.getProperty("randomforest.fs.strategy")
    val impurity = prop.getProperty("randomforest.impurity")
    val maxDepth = prop.getProperty("randomforest.depth.max").toInt
    val maxBins = prop.getProperty("randomforest.bins.max").toInt
    val maxMemory = prop.getProperty("randomforest.memory.max").toInt

    val pipelineArray = Array(service.buildRandomForest(numClasses,numTrees,maxBins,maxDepth,featureSubsetStrategy,impurity,maxMemory))

    val result = evaluator.evaluateAlgorithmSetup(datasets,pipelineArray)
    println("resulting accuracy: " + result)
    writer.write("resulting accuracy:" + result + "\n")

    println("evaluate particular feautures: ")
    writer.write("evaluate particular feautures:\n")

    featuresNames.foreach(x => {
      val result = evaluator.evaluateFeature(datasets, pipelineArray, featuresNames.indexOf(x))
      writer.write("Feature " + x + "accuracy is: " + result + "\n")
      println("Feature " + x + "accuracy is: " + result)
    })

    featuresNames.foreach(x => {
      val result = evaluator.evaluateAgainstFeature(datasets, pipelineArray, featuresNames.indexOf(x), numberOfFeautures)
      writer.write("All against Feature " + x + "accuracy is: " + result + "\n")
      println("All against Feature " + x + "accuracy is: " + result)
    })
    writer close()

  }

  def firstLine(f: java.io.File): Option[String] = {
    val src = Source.fromFile(f)
    try {
      src.getLines.find(_ => true)
    } finally {
      src.close()
    }
  }

}
