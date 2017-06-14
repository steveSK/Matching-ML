package matching.ml

import java.io.FileInputStream
import java.util.Properties

import com.typesafe.config.Config
import matching.lucene.analyzers.SkipGramAnalyzerWithTokenizer
import matching.ml.exception.JobConfigException
import matching.ml.sparkapi.{AlgorithmEvaluator, SparkService}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.scalactic._
import spark.jobserver.api.{JobEnvironment, SingleProblem, SparkJob, ValidationProblem}

import scala.collection.JavaConversions._
import scalax.file.Path

/**
  * A Spark batch job to train a machine learning model, reading from local-file system and storing in also to local file system.
  * currently supported just random forest and logistic regression
  *
  * Configuration loaded dynamicaly via spark-job server using HTTP json POST message
  *
  * Created by stefan on 4/12/17.
  */
class MatchingBatchMLJob extends SparkJob{
  override type JobData = Config
  override type JobOutput = Any

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

  private object MLAlgorithm extends Enumeration{
    type MLAlgorithm  = Value
    val RANDOMFOREST,LOGREGRESS = Value
  }

  val analyzer = new SkipGramAnalyzerWithTokenizer(1, 3)
 /* val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "192.158.0.145:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "10",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean),
    "request.timeout.ms" -> (40000: java.lang.Integer),
    "heartbeat.interval.ms" -> (3000: java.lang.Integer),
    "auto.commit.interval.ms" -> (500: java.lang.Integer)
  ) */


  val service = SparkService
  val evaluator = new AlgorithmEvaluator

  override def runJob(sc: SparkContext, runtime: JobEnvironment, config: JobData): JobOutput = {
    System.out.println("Start batch job:")

    val mlAlgorithm = config.getString(ML_ALGORITHM_CONFIG)
    val modelDestination = config.getString(MODEL_DESTINATION_CONFIG)
    val ignoredColumns =  config.getStringList(IGNORED_COLUMNS_CONFIG).toList
    val labelName = config.getString(LABEL_NAME_CONFIG)
    val featuredDataSetFile = config.getString(FEATURED_DATASET_FILE_CONFIG)

    val rawDataset = service.loadDataFromCSV(featuredDataSetFile,";",true)
    val trainDataset = service.transformRawDFToVectorDF(rawDataset,ignoredColumns,LABEL_COLUMN,true)
    trainDataset.cache()

    val pipelineArray = MLAlgorithm.withName(mlAlgorithm.toUpperCase()) match {
      case MLAlgorithm.RANDOMFOREST => {
        val numClasses = config.getString(RANDOM_FOREST_CLASSES_NUMBER_CONFIG).toInt
        val numTrees = config.getString(RANDOM_FOREST_TREES_NUMBER).toInt
        val featureSubsetStrategy = config.getString(RANDOM_FOREST_SUBSET_STRATEGY_CONFIG)
        val impurity = config.getString(RANDOM_FOREST_IMPURITY_CONFIG)
        val maxDepth = config.getString(RANDOM_FOREST_DEPTH_MAX_CONFIG).toInt
        val maxBins = config.getString(RANDOM_FOREST_BINS_MAX_CONFIG).toInt
        val maxMemory = config.getString(RANDOM_FOREST_MEMORY_MAX_CONFIG).toInt
        Array(service.buildRandomForest(numClasses, numTrees, maxBins, maxDepth, featureSubsetStrategy, impurity, maxMemory))
      }
      case MLAlgorithm.LOGREGRESS => {
        val maxIter = config.getString(LOGREGRESS_ITER_MAX_CONFIG).toDouble.toInt
        val regParam = config.getString(LOGREGRESS_REGPARAM_CONFIG).toDouble
        val elesticNetParam = config.getString(LOGREGRESS_ELASTIC_CONFIG).toDouble
        val treshold = config.getString(LOGREGRESS_TRESHOLD_CONFIG).toDouble
        val family = config.getString(LOGREGRESS_FAMILY_CONFIG)
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

  override def validate(sc: SparkContext, runtime: JobEnvironment, config: Config): Or[JobData, Every[ValidationProblem]] = {
    if(!config.isEmpty){
      Good(config)
    }
    else{
      Bad(One(SingleProblem("config is empty")))
    }
  }
}
