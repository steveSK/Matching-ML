package matching
import java.io.{File, FileInputStream, PrintWriter}
import java.util.Properties

import com.typesafe.config.{Config, ConfigRenderOptions}
import matching.lucene.analyzers.SkipGramAnalyzerWithTokenizer
import matching.ml.spark.{AlgorithmEvaluator, SparkService}
import org.apache.spark.SparkContext
import org.scalactic._
import spark.jobserver.api.{JobEnvironment, SingleProblem, SparkJob, ValidationProblem}

import scala.io.Source
import scala.util.Try

/**
  * Created by stefan on 3/21/17.
  */
class MatchingMLJob extends SparkJob {

  type JobData = Config
  type JobOutput = Any
  private val analyzer = new SkipGramAnalyzerWithTokenizer(1, 3)

  override def runJob(sc: SparkContext, runtime: JobEnvironment, config: JobData): Any = {
    val inputFile = config.getValue("input.file.set").render(ConfigRenderOptions.concise());
    val cleanedInputFile = config.getString("input.file.cleaned")
    val outputFPFile = config.getString("output.file.fp")
    val outputTPFile = config.getString("output.file.tp")
    val outputBorderCases = config.getString("output.file.bordercases")
    val labeledOutputFile = config.getString("output.file.labeled")
    val stopWordsFile = config.getString("input.stopwords")
    val featuredDataSetFile = config.getString("output.file.feature.dataset")
    val names = config.getString("input.file.names")
    val frequencyNameFile = config.getString("output.file.frequencies")
    val similarityRatio = config.getString("similarity.ratio").toDouble
    val numberOfFeautures = config.getString("features.number").toInt

    val service = new SparkService
    val evaluator = new AlgorithmEvaluator(service)
    val ignored = List("string1", "string2", "label")
    val labelName = "label"
    val rawDataset = service.loadDataFromCSV(featuredDataSetFile)
    val featuresNames = rawDataset.columns.filter(x => !ignored.contains(x))
    val datasets = service.getTrainAndTestData(rawDataset, Array(0.9, 0.1), 42, ignored,labelName)
    val fullDataset = datasets._1 ++ datasets._2

    val writer = new PrintWriter(config.getString("output.file.result"))

    val numClasses = config.getString("randomforest.classes.number").toInt
    val numTrees = config.getString("randomforest.trees.number").toInt
    val featureSubsetStrategy = config.getString("randomforest.fs.strategy")
    val impurity = config.getString("randomforest.impurity")
    val maxDepth = config.getString("randomforest.depth.max").toInt
    val maxBins = config.getString("randomforest.bins.max").toInt
    val maxMemory = config.getString("randomforest.memory.max").toInt

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
    writer.close()
  }

  override def validate(sc: SparkContext, runtime: JobEnvironment, config: Config): Or[JobData, Every[ValidationProblem]] = {
    if(!config.isEmpty){
       Good(config)
    }
    else{
      Bad(One(SingleProblem("config is empty")))
    }
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

