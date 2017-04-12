package matching.ml

import java.io.FileInputStream
import java.util.Properties

import matching.lucene.analyzers.SkipGramAnalyzerWithTokenizer
import matching.ml.sparkapi.{AlgorithmEvaluator, SparkService}
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * Created by stefan on 4/11/17.
  */
object MatchingBatchMLMain {
  val analyzer = new SkipGramAnalyzerWithTokenizer(1, 3)
  val prop = new Properties()
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


  def main(args: Array[String]) {
    System.out.println("Start batch job:")
    val propertyFile = args(0)
    prop.load(new FileInputStream(propertyFile))
    val similarityRatio = prop.getProperty("similarity.ratio").toDouble
    val numberOfFeautures = prop.getProperty("features.number").toInt
    val modelDestination = prop.getProperty("model.destination")
    val ignored =  prop.getProperty("dataset.column.ignored").split(",").toList
    val labelName = prop.getProperty("dataset.column.label")
    val datasetSize  = prop.getProperty("dataset.size").toInt
    val datasetTopic = prop.getProperty("dataset.topic")

    val rawDataset = service.loadDataFromKafka(datasetTopic,Array((0,datasetSize)),kafkaParams)
    rawDataset.cache()
    val trainDataset = service.transformRawDFToVectorDF(rawDataset,labelName,ignored)

    val numClasses = prop.getProperty("randomforest.classes.number").toInt
    val numTrees = prop.getProperty("randomforest.trees.number").toInt
    val featureSubsetStrategy = prop.getProperty("randomforest.fs.strategy")
    val impurity = prop.getProperty("randomforest.impurity")
    val maxDepth = prop.getProperty("randomforest.depth.max").toInt
    val maxBins = prop.getProperty("randomforest.bins.max").toInt
    val maxMemory = prop.getProperty("randomforest.memory.max").toInt


    val pipelineArray = Array(service.buildRandomForest(numClasses, numTrees, maxBins, maxDepth, featureSubsetStrategy, impurity, maxMemory))
    val model = service.buildModel(trainDataset, pipelineArray)
    model.save(modelDestination)

  }
}
