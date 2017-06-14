package matching.ml

import java.io.FileInputStream
import java.util.Properties

import matching.ml.datamining.worldcheck.FeatureGenerator
import matching.ml.sparkapi.{KafkaSink, SparkService}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Copy of MatchingStreamMLJob for local debugging
  *
  * Created by stefan on 4/11/17.
  */
object MatchingStreamingMLMain {

  private val MODEL_DESTINATION_CONFIG = "matching.model.destination"
  private val ALIASES_TOMATCH_FILE_CONFIG = "dataset.aliases.file"
  private val IGNORED_COLUMNS_CONFIG = "dataset.columns.ignored"
  private val STREAM_TOPIC_CONFIG = "input.topic"
  private val RESULT_TOPIC_CONFIG = "output.topic"
  private val UID_COLUMN= "uid"
  private val CITIZENSHIP_COLUMN = "citizenship"
  private val LABEL_COLUMN = "label"
  private val PAIR_KEY1_NAME = "string1"
  private val PAIR_KEY2_NAME = "string2"
  private val OUTPUT_DELIMETER = ","

  val prop = new Properties()
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "192.158.0.145:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "key.serializer" -> classOf[StringSerializer],
    "value.serializer" -> classOf[StringSerializer],
    "group.id" -> "10",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean),
    "request.timeout.ms" -> (40000: java.lang.Integer),
    "heartbeat.interval.ms" -> (3000: java.lang.Integer),
    "auto.commit.interval.ms" -> (500: java.lang.Integer)
  )
  val service = SparkService
  val streamingDelimeter = ";"

  def main(args: Array[String]) {
    System.out.println("Start streaming job")
    val propertyFile = args(0)
    prop.load(new FileInputStream(propertyFile))
    val modelDestination =  prop.getProperty(MODEL_DESTINATION_CONFIG)
    val aliasesToMatchFile =  prop.getProperty(ALIASES_TOMATCH_FILE_CONFIG)
    val ignoredColumns =   prop.getProperty(IGNORED_COLUMNS_CONFIG )
    val streamTopic =  prop.getProperty(STREAM_TOPIC_CONFIG)
    val resultTopic =  prop.getProperty(RESULT_TOPIC_CONFIG)

    val model = PipelineModel.load(modelDestination)

    val streamDSRaw = service.createKafkaStreamStringDataSet(streamTopic,kafkaParams)
    val kafkaSink = service.sparkSession.sparkContext.broadcast(KafkaSink(kafkaParams))

    val query = streamDSRaw.writeStream.foreach(new ForeachWriter[String] {
      override def open(partitionId: Long, version: Long): Boolean = {
        println("open("+partitionId+","+version+")")
        true
      }

      override def process(record: String) : Unit = {
        val name = record.split(streamingDelimeter)(0).replace("\"","")
        val country = record.split(streamingDelimeter)(1).replace("\"","")
        // for debuging
        println("process name: " + name)
        println("blocking key: " + country)
        println("file " + aliasesToMatchFile)
        val featuredDS = SparkService.transformRawDFToVectorDF(
          FeatureGenerator.generateFeatureForMatchAgainstFile(name,country,aliasesToMatchFile,";"),ignoredColumns.split(",").toList,LABEL_COLUMN,false)
        println("success")
        println("match against: " + featuredDS.count())
        val modeApplied = model.transform(featuredDS)
        val indexPredictColumn = modeApplied.columns.indexOf("prediction")

        val results = modeApplied.filter(x => x.getDouble(2) == 1.0).select(PAIR_KEY2_NAME,UID_COLUMN,CITIZENSHIP_COLUMN)
        results.foreach(x => kafkaSink.value.send(resultTopic,x.mkString(OUTPUT_DELIMETER)))
      }

      override def close(errorOrNull: Throwable): Unit = {
        // close the connection
        println("close()")
      }
    }).start()

    query.awaitTermination()


  }

}
