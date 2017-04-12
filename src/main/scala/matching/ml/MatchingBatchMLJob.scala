package matching.ml

import java.io.FileInputStream
import java.util.Properties

import com.typesafe.config.Config
import matching.lucene.analyzers.SkipGramAnalyzerWithTokenizer
import matching.ml.sparkapi.{AlgorithmEvaluator, SparkService}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.scalactic._
import spark.jobserver.api.{JobEnvironment, SingleProblem, SparkJob, ValidationProblem}

/**
  * Created by stefan on 4/12/17.
  */
class MatchingBatchMLJob extends SparkJob{
  override type JobData = Config
  override type JobOutput = Any

  val analyzer = new SkipGramAnalyzerWithTokenizer(1, 3)
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


  val service = new SparkService
  val evaluator = new AlgorithmEvaluator(service)

  override def runJob(sc: SparkContext, runtime: JobEnvironment, config: JobData): JobOutput = {
    System.out.println("Start batch job:")
    val similarityRatio = config.getString("similarity.ratio").toDouble
    val numberOfFeautures = config.getString("features.number").toInt
    val modelDestination = config.getString("model.destination")
    val ignored =  config.getString("dataset.column.ignored").split(",").toList
    val labelName = config.getString("dataset.column.label")
    val datasetSize  = config.getString("dataset.size").toInt
    val datasetTopic = config.getString("dataset.topic")

    val rawDataset = service.loadDataFromKafka(datasetTopic,Array((0,datasetSize)),kafkaParams)
    rawDataset.cache()
    val trainDataset = service.transformRawDFToVectorDF(rawDataset,labelName,ignored)

    val numClasses = config.getString("randomforest.classes.number").toInt
    val numTrees = config.getString("randomforest.trees.number").toInt
    val featureSubsetStrategy = config.getString("randomforest.fs.strategy")
    val impurity = config.getString("randomforest.impurity")
    val maxDepth = config.getString("randomforest.depth.max").toInt
    val maxBins = config.getString("randomforest.bins.max").toInt
    val maxMemory = config.getString("randomforest.memory.max").toInt

    val pipelineArray = Array(service.buildRandomForest(numClasses, numTrees, maxBins, maxDepth, featureSubsetStrategy, impurity, maxMemory))
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
