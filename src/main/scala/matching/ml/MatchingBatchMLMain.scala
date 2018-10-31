package matching.ml

import java.io.FileInputStream
import java.util.Properties

import matching.lucene.analyzers.SkipGramAnalyzerWithTokenizer
import matching.ml.exception.JobConfigException
import matching.ml.sparkapi.{AlgorithmEvaluator, SparkService}
import org.apache.kafka.common.serialization.StringDeserializer

import scalax.file.Path

/**
  *  Copy of MatchingBatchMLJob for local debugging (running spark locally without spark-job server)
  * Created by stefan on 4/11/17.
  */
object MatchingBatchMLMain {
  val analyzer = new SkipGramAnalyzerWithTokenizer(1, 3)
  val prop = new Properties()

  private val ML_ALGORITHM_CONFIG = "matching.ml.algorithm"
  private val MODEL_DESTINATION_CONFIG = "matching.model.destination"
  private val IGNORED_COLUMNS_CONFIG  = "dataset.columns.ignored"
  private val LABEL_NAME_CONFIG  = "dataset.columns.label"
  private val FEATURED_DATASET_FILE_CONFIG = "dataset.local.classpath"

  private val RANDOM_FOREST_CLASSES_NUMBER_CONFIG = "randomforest.classes.number"
  private val RANDOM_FOREST_TREES_NUMBER = "randomforest.trees.number"
  private val RANDOM_FOREST_SUBSET_STRATEGY_CONFIG = "randomforest.fs.strategy"
  private val RANDOM_FOREST_IMPURITY_CONFIG = "randomforest.impurity"
  private val RANDOM_FOREST_DEPTH_MAX_CONFIG = "randomforest.depth.max"
  private val RANDOM_FOREST_BINS_MAX_CONFIG = "randomforest.bins.max"
  private val RANDOM_FOREST_MEMORY_MAX_CONFIG = "randomforest.memory.max"

  private val LOGREGRESS_ITER_MAX_CONFIG = "logregression.iter.max"
  private val LOGREGRESS_REGPARAM_CONFIG = "logregression.regparam"
  private val LOGREGRESS_ELASTIC_CONFIG = "logregression.elasticnetparam"
  private val LOGREGRESS_TRESHOLD_CONFIG = "logregression.treshold"
  private val LOGREGRESS_FAMILY_CONFIG = "logregression.family"

  private val LABEL_COLUMN = "label"


  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "192.158.0.145:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "10",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean),
    "request.timeout.ms" -> (40000: java.lang.Integer),
    "heartbeat.interval.ms" -> (3000: java.lang.Integer),
    "auto.commit.interval.ms" -> (500: java.lang.Integer)
  )

  private object MLAlgorithm extends Enumeration{
    type MLAlgorithm  = Value
    val RANDOMFOREST,LOGREGRESS = Value
  }


  val service = SparkService
  val evaluator = new AlgorithmEvaluator


  def main(args: Array[String]) {
    System.out.println("Start batch job:")
    val propertyFile = args(0)
    prop.load(new FileInputStream(propertyFile))
    System.out.println("Start batch job:")

    val mlAlgorithm = prop.getProperty(ML_ALGORITHM_CONFIG)
    val modelDestination = prop.getProperty(MODEL_DESTINATION_CONFIG)
    val ignoredColumns =  prop.getProperty(IGNORED_COLUMNS_CONFIG).split(",").toList
    val labelName = prop.getProperty(LABEL_NAME_CONFIG)
    val featuredDataSetFile = prop.getProperty(FEATURED_DATASET_FILE_CONFIG)

    val rawDataset = service.loadDataFromCSV(featuredDataSetFile,";",true)
    val trainDataset = service.transformRawDFToVectorDF(rawDataset,ignoredColumns,LABEL_COLUMN,true)
    trainDataset.cache()

    val pipelineArray = MLAlgorithm.withName(mlAlgorithm.toUpperCase()) match {
      case MLAlgorithm.RANDOMFOREST => {
        val numClasses = prop.getProperty(RANDOM_FOREST_CLASSES_NUMBER_CONFIG).toInt
        val numTrees = prop.getProperty(RANDOM_FOREST_TREES_NUMBER).toInt
        val featureSubsetStrategy = prop.getProperty(RANDOM_FOREST_SUBSET_STRATEGY_CONFIG)
        val impurity = prop.getProperty(RANDOM_FOREST_IMPURITY_CONFIG)
        val maxDepth = prop.getProperty(RANDOM_FOREST_DEPTH_MAX_CONFIG).toInt
        val maxBins = prop.getProperty(RANDOM_FOREST_BINS_MAX_CONFIG).toInt
        val maxMemory = prop.getProperty(RANDOM_FOREST_MEMORY_MAX_CONFIG).toInt
        Array(service.buildRandomForest(numClasses, numTrees, maxBins, maxDepth, featureSubsetStrategy, impurity, maxMemory))
      }
      case MLAlgorithm.LOGREGRESS => {
        val maxIter = prop.getProperty(LOGREGRESS_ITER_MAX_CONFIG).toDouble.toInt
        val regParam = prop.getProperty(LOGREGRESS_REGPARAM_CONFIG).toDouble
        val elesticNetParam = prop.getProperty(LOGREGRESS_ELASTIC_CONFIG).toDouble
        val treshold = prop.getProperty(LOGREGRESS_TRESHOLD_CONFIG).toDouble
        val family = prop.getProperty(LOGREGRESS_FAMILY_CONFIG)
        Array(service.buildLogisticRegression(maxIter,regParam,elesticNetParam,family,treshold))
      }
      case _ => {
        throw new JobConfigException("ml algorithm " + mlAlgorithm + " is not supported")
      }
    }
    // remove if model dir exists
    if(new java.io.File(modelDestination).exists()){
      val path = Path.fromString(modelDestination)
      path.deleteRecursively(false)
    }
    //build the model
    val model = service.buildModel(trainDataset, pipelineArray)
    model.save(modelDestination)

  }
}
