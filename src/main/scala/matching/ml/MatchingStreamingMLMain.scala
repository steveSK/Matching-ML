package matching.ml

import java.io.FileInputStream
import java.util.Properties

import matching.ml.sparkapi.SparkService
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by stefan on 4/11/17.
  */
object MatchingStreamingMLMain {

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

  def main(args: Array[String]) {
    System.out.println("Start streaming job")
    val propertyFile = args(0)
    prop.load(new FileInputStream(propertyFile))
    val modelDestination = prop.getProperty("model.destination")
    val ignored =  prop.getProperty("dataset.column.ignored").split(",").toList
    val columnNames = prop.getProperty("dataset.column.names").split(",")
    val labelName = prop.getProperty("dataset.column.label")
    val streamTopic = prop.getProperty("stream.topic")


    val schema = StructType(columnNames.map(x => StructField(x,StringType)))

    val streamDSRaw = service.createKafkaStreamDataSet(streamTopic,kafkaParams,columnNames,schema)
    val streamDS = service.transformRawDFToVectorDF(streamDSRaw,labelName,ignored)

    val model = PipelineModel.load(modelDestination)
    val predictions = model.transform(streamDS).select("prediction")
    val query = predictions.writeStream.format("console").start()
    query.awaitTermination()


  }

}
