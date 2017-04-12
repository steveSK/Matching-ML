package matching.ml.sparkapi

import java.lang.Exception

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml._
import org.apache.spark.ml.classification.{LogisticRegression, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.classification.{ClassificationModel, NaiveBayesModel}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.functions.from_json

import collection.JavaConverters._

//import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Created by stefan on 12/19/16.
  */
class SparkService() {


  val sparkSession = SparkSession.builder().getOrCreate()
  sparkSession.sqlContext.setConf("matching.spark.sql.shuffle.partitions", "6")
  sparkSession.sqlContext.setConf("spark.streaming.kafka.consumer.poll.ms","2000");
  sparkSession.sqlContext.setConf("spark.streaming.kafka.maxRatePerPartition","1");
  val streamingContext = new StreamingContext(sparkSession.sparkContext,Seconds(1))
  val JSON_STRUCT_LABEL = "jsontostruct(CAST(value AS STRING))"

  import sparkSession.implicits._


  def buildNaiveBayesModelMLlib(dataset: RDD[org.apache.spark.mllib.regression.LabeledPoint]) : NaiveBayesModel = {
    val lambda = 0.01
    val modelType = "multinomial"
    return org.apache.spark.mllib.classification.NaiveBayes.train(dataset, lambda, modelType)
  }

  def buildModel(dataset: Dataset[_],stages: Array[_ <: PipelineStage]): PipelineModel = {
    val pipeline = new Pipeline().setStages(stages)
    return pipeline.fit(dataset)
  }

  def buildModel(dataset: RDD[LabeledPoint],stages: Array[_ <: PipelineStage]): PipelineModel = {
    val pipeline = new Pipeline().setStages(stages)
    return pipeline.fit(dataset.toDF("label","features"))
  }

  def loadModel(pathToModel: String): PipelineModel ={
    try {
      PipelineModel.load(pathToModel)
    } catch {
      case e: Exception => null
    }
  }

  def buildNaiveBayes() : NaiveBayes = {
    val modelType = "multinomial"
    return new NaiveBayes().setModelType(modelType);
  }

  def buildRandomForest(numClasses: Int, numTrees: Int, maxBins: Int, maxDepth: Int, featureSubsetStrategy: String, impurity: String, maxMemory: Int): RandomForestClassifier = {
    val categoricalFeaturesInfo = Map[Int, Int]()
    return new RandomForestClassifier()
      .setNumTrees(numTrees)
      .setMaxBins(maxBins)
      .setMaxDepth(maxDepth)
      .setImpurity(impurity)
      .setMaxMemoryInMB(maxMemory)
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setFeatureSubsetStrategy(featureSubsetStrategy)
  }

  def buildLogisticRegression() : LogisticRegression = {
    val maxIter = 10
    val regParam = 0.3
    val elesticNetParam = 0.8

    return new LogisticRegression()
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setElasticNetParam(elesticNetParam)
  }


  def evaluateTestDataMLlib(testDF: RDD[org.apache.spark.mllib.regression.LabeledPoint], model :  ClassificationModel): Double ={
    val labelAndPreds = testDF.map { point =>
      val prediction = model.predict(org.apache.spark.mllib.linalg.Vectors.dense(point.features.toArray))
      (point.label, prediction)
    }

    val metrics = new BinaryClassificationMetrics(labelAndPreds)
    return metrics.areaUnderROC()
  }

  def createKafkaStream(topic: String, kafkaParams : Map[String,Object]) = {
    KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      Subscribe[String, String](Array(topic), kafkaParams))
     sparkSession
  }

  def createKafkaStreamDataSet(topic: String, kafkaParams : Map[String,Object],columns: Array[String],schema: StructType): Dataset[Row] ={
    val rawKafkaDF = sparkSession.sqlContext.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","192.158.0.145:9092")
      .option("subscribe",topic)
      .load()
   // val columnsToSelect = columns.map( x => new Column("value." + x))
    rawKafkaDF.select(from_json($"value".cast(StringType),schema)).select(flattenSchema(schema,JSON_STRUCT_LABEL) : _*)


  }

  def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)
      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(new Column(colName))
      }
    })
  }


  def evaluateTestDataML(testDF: Dataset[_], model :  Transformer): Double ={
    val predictions = model.transform(testDF.toDF())
    predictions.select("prediction", "label", "features").show(5)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    return evaluator.evaluate(predictions)
  }

  def crossValidate(trainDF: DataFrame, paramGrid: Array[ParamMap], pipeline: Estimator[_], nFolds : Int) : Transformer = {
    val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")
    pipeline
    val cv = new CrossValidator()
      // ml.Pipeline with ml.classification.RandomForestClassifier
      .setEstimator(pipeline)
      // ml.evaluation.MulticlassClassificationEvaluator
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(nFolds)

    return cv.fit(trainDF)
  }

  def getTrainAndTestData(rawDF : Dataset[Row], weights: Array[Double], seed: Int, ignored: List[String], labelName: String): (RDD[LabeledPoint],
    RDD[LabeledPoint]) ={
    val listDFs =  rawDF.randomSplit(weights,seed).map(x => x.toDF())
    val trainDF = listDFs(0)
    val testDF = listDFs(1)

    return (transformRawDFToLabeledPointRDDML(trainDF,labelName,ignored), transformRawDFToLabeledPointRDDML(testDF,labelName,ignored))
  }

  def getTrainAndTestDataDF(rawDF : Dataset[Row], weights: Array[Double], seed: Int, ignored: List[String], labelName: String): (Dataset[Row],
    Dataset[Row]) ={
    val listDFs =  rawDF.randomSplit(weights,seed).map(x => x.toDF())
    val trainDF = listDFs(0)
    val testDF = listDFs(1)

    return (transformRawDFToVectorDF(trainDF,labelName,ignored), transformRawDFToVectorDF(testDF,labelName,ignored))
  }


  def getTrainAndTestDataMLlib(rawDF : Dataset[Row], weights: Array[Double], seed: Int, ignored: List[String], labelName: String): (RDD[org.apache.spark.mllib.regression.LabeledPoint],
    RDD[org.apache.spark.mllib.regression.LabeledPoint]) ={
    val listDFs =  rawDF.randomSplit(weights,seed).map(x => x.toDF())
    val trainDF = listDFs(0)
    val testDF = listDFs(1)

    return (transformRawDFToLabeledPointRDDMLlib(trainDF,labelName,ignored), transformRawDFToLabeledPointRDDMLlib(testDF,labelName,ignored))
  }

  def loadDataFromCSV(filepath: String): Dataset[Row] = {
    sparkSession.read.option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .format("csv").load(filepath)
  }

  def loadDataFromKafka(topic: String,offsets: Array[(Int,Int)], kafkaParams : Map[String,Object]): Dataset[Row] = {
      val offsetRange = offsets.map(x => OffsetRange(topic,x._1,0,x._2))
      val rdd = KafkaUtils.createRDD[String,String](sparkSession.sparkContext,kafkaParams.asJava,offsetRange,LocationStrategies.PreferConsistent).map(x => x.value())
      sparkSession.read.json(rdd)
  }

  def transformRawDFToLabeledPointRDDML(df: Dataset[Row], labelName: String,ignored: List[String]): RDD[LabeledPoint] = {
     val featInd = df.columns.diff(ignored).map(df.columns.indexOf(_))
     val labelIndex = df.columns.indexOf(labelName)
     df.rdd.map(x => LabeledPoint(x.getString(labelIndex).toDouble, org.apache.spark.ml.linalg.Vectors.dense(featInd.map(x.get(_).toString.toDouble))))
  }


  def transformRawDFToLabeledPointRDDMLlib(df: Dataset[Row], labelName: String,ignored: List[String]): RDD[org.apache.spark.mllib.regression.LabeledPoint] = {
    val featInd = df.columns.diff(ignored).map(df.columns.indexOf(_))
    val labelIndex = df.columns.indexOf(labelName)
    df.rdd.map(x => org.apache.spark.mllib.regression.LabeledPoint(x.getString(labelIndex).toDouble, org.apache.spark.mllib.linalg.Vectors.dense(featInd.map(x.get(_).toString.toDouble))))

  }

  def transformRawDFToVectorDF(df: Dataset[Row],labelName: String): Dataset[Row] = {
    transformRawDFToVectorDF(df, labelName, List())
  }


    def transformRawDFToVectorDF(df: Dataset[Row],labelName: String, ignored: List[String]): Dataset[Row] ={
    val featInd = df.columns.diff(ignored).map(df.columns.indexOf(_))
    val labelIndex = df.columns.indexOf(labelName)
    val schema = StructType(Seq(
      StructField("label", DoubleType),
      StructField("features", VectorType)
    ))

    val encoder = RowEncoder(schema)
    df.map(x => Row(x.getString(labelIndex).toDouble,org.apache.spark.ml.linalg.Vectors.dense(featInd.map(x.get(_).toString.toDouble))))(encoder).toDF("label","features")

  }

  def getSparkSession(): Unit = {
    sparkSession
  }


}
