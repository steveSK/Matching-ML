package matching.ml


import java.io.FileInputStream

import com.typesafe.config.Config
import matching.ml.datamining.worldcheck.FeatureGenerator
import matching.ml.sparkapi.{KafkaSink, SparkService}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{Dataset, ForeachWriter, Row}
import org.scalactic._

import scala.collection.JavaConversions._
import spark.jobserver.api.{JobEnvironment, SingleProblem, SparkJob, ValidationProblem}

/**
  * A spark stream job or real-time entity resolution. It implements SparkJob-server API.
  * It loads file from local file system to match it against incoming records from Kafka.
  * Resulting matches are sent back to Kafka to result topic
  *
  * Created by stefan on 4/12/17.
  */
class MatchingStreamMLJob extends SparkJob {
  override type JobData = Config
  override type JobOutput = Any

  private val MODEL_DESTINATION_CONFIG = "matching.model.destination"
  private val ALIASES_TOMATCH_FILE_CONFIG = "dataset.aliases.file"
  private val IGNORED_COLUMNS_CONFIG = "dataset.columns.ignored"
  private val STREAM_TOPIC_CONFIG = "input.topic"
  private val RESULT_TOPIC_CONFIG = "output.topic"
  private val UID_COLUMN= "uid"
  private val CITIZENSHIP_COLUMN = "citizenship"
  private val CONFIG_PREFIX = "kafka"
  private val LABEL_COLUMN = "label"
  private val PAIR_KEY1_NAME = "string1"
  private val PAIR_KEY2_NAME = "string2"
  private val OUTPUT_DELIMETER = ","

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

  val streamingDelimeter = ";"

  override def runJob(sc: SparkContext, runtime: JobEnvironment, config: JobData): JobOutput = {
    System.out.println("Start streaming job")

    val modelDestination = config.getString(MODEL_DESTINATION_CONFIG)
    val aliasesToMatchFile = config.getString(ALIASES_TOMATCH_FILE_CONFIG)
    val ignoredColumns =  config.getStringList( IGNORED_COLUMNS_CONFIG).toList
    val streamTopic = config.getString(STREAM_TOPIC_CONFIG)
    val outputTopic = config.getString(RESULT_TOPIC_CONFIG)

    val kafkaConfig = config.getConfig(CONFIG_PREFIX)
    val kafkaParams = kafkaConfig.entrySet().map(x => x.getKey -> x.getValue.render()).toMap

    val model = PipelineModel.load(modelDestination)


    val streamDSRaw = service.createKafkaStreamStringDataSet(streamTopic,kafkaParams)
    val kafkaSink = sc.broadcast(KafkaSink(kafkaParams))

    val query = streamDSRaw.writeStream.foreach(new ForeachWriter[String] {
      override def open(partitionId: Long, version: Long): Boolean = {
        println("open("+partitionId+","+version+")")
        true
      }
      // match against aliases available in a file, block according the country and send resulting matches back to Kafka
      override def process(record: String) : Unit = {
        val name = record.split(streamingDelimeter)(0).replace("\"","")
        val country = record.split(streamingDelimeter)(1).replace("\"","")
        val featuredDS = SparkService.transformRawDFToVectorDF(
          FeatureGenerator.generateFeatureForMatchAgainstFile(name,country,aliasesToMatchFile,";"),ignoredColumns,LABEL_COLUMN,false)

        val modeApplied = model.transform(featuredDS)
        val indexPredictColumn = modeApplied.columns.indexOf("prediction")
        val results = modeApplied.filter(x => x.getDouble(2) == 1.0).select(PAIR_KEY2_NAME,UID_COLUMN,CITIZENSHIP_COLUMN)
       results.foreach(x => kafkaSink.value.send(outputTopic,x.mkString(OUTPUT_DELIMETER)))
      }

      override def close(errorOrNull: Throwable): Unit = {
        println("close()")
      }
    }).start()

    query.awaitTermination()

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
