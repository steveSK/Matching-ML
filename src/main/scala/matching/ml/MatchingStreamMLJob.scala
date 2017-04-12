package matching.ml


import java.io.FileInputStream

import com.typesafe.config.Config
import matching.ml.sparkapi.SparkService
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalactic._
import spark.jobserver.api.{JobEnvironment, SingleProblem, SparkJob, ValidationProblem}

/**
  * Created by stefan on 4/12/17.
  */
class MatchingStreamMLJob extends SparkJob {
  override type JobData = Config
  override type JobOutput = Any

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "192.158.0.145:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "11",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean),
    "request.timeout.ms" -> (40000: java.lang.Integer),
    "heartbeat.interval.ms" -> (3000: java.lang.Integer),
    "auto.commit.interval.ms" -> (500: java.lang.Integer)
  )
  val service = new SparkService

  override def runJob(sc: SparkContext, runtime: JobEnvironment, config: JobData): JobOutput = {
    System.out.println("Start streaming job")

    val modelDestination = config.getString("model.destination")
    val ignored =  config.getString("dataset.column.ignored").split(",").toList
    val columnNames = config.getString("dataset.column.names").split(",")
    val labelName = config.getString("dataset.column.label")
    val streamTopic = config.getString("stream.topic")

    val schema = StructType(columnNames.map(x => StructField(x,StringType)))

    val streamDSRaw = service.createKafkaStreamDataSet(streamTopic,kafkaParams,columnNames,schema)
    val streamDS = service.transformRawDFToVectorDF(streamDSRaw,labelName,ignored)

    val model = PipelineModel.load(modelDestination)
    val predictions = model.transform(streamDS).select("prediction")
    val query = predictions.writeStream.format("console").start()
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
