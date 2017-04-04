package matching.ml.spark

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
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.kafka010._
import collection.JavaConverters._

//import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Created by stefan on 12/19/16.
  */
class SparkService(sc: SparkContext) {


  val sparkSession = SparkSession.builder().getOrCreate()
  sparkSession.sqlContext.setConf("matching.spark.sql.shuffle.partitions", "6")
  sparkSession.sqlContext.setConf("spark.streaming.kafka.consumer.poll.ms","2000");
  import sparkSession.implicits._


  def buildNaiveBayesModelMLlib(dataset: RDD[org.apache.spark.mllib.regression.LabeledPoint]) : NaiveBayesModel = {
    val lambda = 0.01
    val modelType = "multinomial"
    return org.apache.spark.mllib.classification.NaiveBayes.train(dataset, lambda, modelType)
  }

  def buildModel(dataset: Dataset[_],pipeline: Pipeline): Transformer = {
    return pipeline.fit(dataset)
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

 /* def makeGrid( estimator:Estimator[_] ): ParamGridBuilder= {
    val paramMap = estimator.extractParamMap();
    val param1 = estimator.getParam("hashingTF");
    val param2 = estimator.getParam("regParam");
    new ParamGridBuilder()
      .addGrid(paramMap(param1), Array(10, 100, 1000))
      .addGrid(paramMap(param2), Array(0.1, 0.01))
      .build()
  } */

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

  def getTrainAndTestDataFromFileML(datasetFile: String, weights: Array[Double], seed: Int, ignored: List[String], labelName: String): (RDD[LabeledPoint],
    RDD[LabeledPoint]) ={
    val rawDF = loadDataFromCSV(datasetFile, sparkSession)
    val listDFs =  rawDF.randomSplit(weights,seed).map(x => x.toDF())
    val trainDF = listDFs(0)
    val testDF = listDFs(1)

    return (transformRawDFToLabeledPointRDDML(trainDF,sparkSession,labelName,ignored), transformRawDFToLabeledPointRDDML(testDF,sparkSession,labelName,ignored))
  }

  def getTrainAndTestDataFromKafka(topic: String,offsets: Array[(Int,Int)], params:Map[String,Object], weights: Array[Double], seed: Int, ignored: List[String], labelName: String): (RDD[LabeledPoint],
    RDD[LabeledPoint]) ={
    val rawDF = loadDataFromKafka(topic,offsets, params, sparkSession)
    rawDF.show()
    println("COUNT IS: " + rawDF.count())
    val listDFs =  rawDF.randomSplit(weights,seed).map(x => x.toDF())
    val trainDF = listDFs(0)
    val testDF = listDFs(1)

    return (transformRawDFToLabeledPointRDDML(trainDF,sparkSession,labelName,ignored), transformRawDFToLabeledPointRDDML(testDF,sparkSession,labelName,ignored))
  }

  def getTrainAndTestDataFromFileMLlib(datasetFile: String, weights: Array[Double], seed: Int, ignored: List[String], labelName: String): (RDD[org.apache.spark.mllib.regression.LabeledPoint],
    RDD[org.apache.spark.mllib.regression.LabeledPoint]) ={
    val rawDF = loadDataFromCSV(datasetFile, sparkSession)
    val listDFs =  rawDF.randomSplit(weights,seed).map(x => x.toDF())
    val trainDF = listDFs(0)
    val testDF = listDFs(1)

    return (transformRawDFToLabeledPointRDDMLlib(trainDF,sparkSession,labelName,ignored), transformRawDFToLabeledPointRDDMLlib(testDF,sparkSession,labelName,ignored))
  }

  def loadDataFromCSV(filepath: String, sparkSession: SparkSession): Dataset[Row] = {
    sparkSession.read.option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .format("csv").load(filepath)
  }

  def loadDataFromKafka(topic: String,offsets: Array[(Int,Int)], kafkaParams : Map[String,Object], sparkSession: SparkSession): Dataset[Row] = {
      val offsetRange = offsets.map(x => OffsetRange(topic,x._1,0,x._2))
      val rdd = KafkaUtils.createRDD[String,String](sparkSession.sparkContext,kafkaParams.asJava,offsetRange,LocationStrategies.PreferConsistent).map(x => x.value())
      sparkSession.read.json(rdd)
  }

  def transformRawDFToLabeledPointRDDML(df: Dataset[Row], sparkSession: SparkSession, labelName: String,ignored: List[String]): RDD[LabeledPoint] = {
     val featInd = df.columns.diff(ignored).map(df.columns.indexOf(_))
     val labelIndex = df.columns.indexOf(labelName)
     df.rdd.map(x => LabeledPoint(x.getDouble(labelIndex), org.apache.spark.ml.linalg.Vectors.dense(featInd.map(x.get(_).toString.toDouble))))
  }


  def transformRawDFToLabeledPointRDDMLlib(df: Dataset[Row], sparkSession: SparkSession, labelName: String,ignored: List[String]): RDD[org.apache.spark.mllib.regression.LabeledPoint] = {
    val featInd = df.columns.diff(ignored).map(df.columns.indexOf(_))
    val labelIndex = df.columns.indexOf(labelName)
    df.rdd.map(x => org.apache.spark.mllib.regression.LabeledPoint(x.getDouble(labelIndex), org.apache.spark.mllib.linalg.Vectors.dense(featInd.map(x.get(_).toString.toDouble))))

  }

  def getSparkSession(): Unit = {
    sparkSession
  }


}
