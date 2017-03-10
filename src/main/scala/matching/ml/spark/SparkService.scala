package matching.ml.spark

import org.apache.spark.SparkConf
import org.apache.spark.ml._
import org.apache.spark.ml.classification.{LogisticRegression, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.classification.{ClassificationModel, NaiveBayesModel}
import org.apache.spark.sql.DataFrame

//import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Created by stefan on 12/19/16.
  */
class SparkService {


  val sparkSession = SparkSession.builder().getOrCreate()
  sparkSession.sqlContext.setConf("matching.spark.sql.shuffle.partitions", "6")
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
   // val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
  //  val numTrees = 30 // Use more in practice.
  //  val featureSubsetStrategy = "auto" // Let the algorithm choose.
  //  val impurity = "entropy"
  //  val maxDepth = 12
 //   val maxBins = 16
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

  def evaluateTestDataML(testDF: Dataset[_], model :  Transformer): Double ={
    val predictions = model.transform(testDF.toDF())
    predictions.select("prediction", "label", "features").show(5)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    return evaluator.evaluate(predictions)
  }

  def crossValidate(trainDF: DataFrame, paramGrid: Array[ParamMap], pipeline: Pipeline, nFolds : Int) : Transformer = {
    val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

    val cv = new CrossValidator()
      // ml.Pipeline with ml.classification.RandomForestClassifier
      .setEstimator(pipeline)
      // ml.evaluation.MulticlassClassificationEvaluator
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(nFolds)

    return cv.fit(trainDF)
  }

  def getTrainAndTestDataFromFileML(datasetFile: String, weights: Array[Double], seed: Int, ignored: List[String]): (RDD[LabeledPoint],
    RDD[LabeledPoint]) ={
    val rawDF = loadDataFromCSV(datasetFile, sparkSession)
    val listDFs =  rawDF.randomSplit(weights,seed).map(x => x.toDF())
    val trainDF = listDFs(0)
    val testDF = listDFs(1)

    return (transformRawDFToLabeledPointRDDML(trainDF,sparkSession,11,ignored), transformRawDFToLabeledPointRDDML(testDF,sparkSession,11,ignored))
  }

  def getTrainAndTestDataFromFileMLlib(datasetFile: String, weights: Array[Double], seed: Int, ignored: List[String]): (RDD[org.apache.spark.mllib.regression.LabeledPoint],
    RDD[org.apache.spark.mllib.regression.LabeledPoint]) ={
    val rawDF = loadDataFromCSV(datasetFile, sparkSession)
    val listDFs =  rawDF.randomSplit(weights,seed).map(x => x.toDF())
    val trainDF = listDFs(0)
    val testDF = listDFs(1)

    return (transformRawDFToLabeledPointRDDMLlib(trainDF,sparkSession,11,ignored), transformRawDFToLabeledPointRDDMLlib(testDF,sparkSession,11,ignored))
  }




   def loadDataFromCSV(filepath: String, sparkSession: SparkSession): Dataset[Row] = {
    sparkSession.read.option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .format("csv").load(filepath)
  }

  def transformRawDFToLabeledPointRDDML(df: Dataset[Row], sparkSession: SparkSession, labelIndex: Int,ignored: List[String]): RDD[LabeledPoint] = {
     val featInd = df.columns.diff(ignored).map(df.columns.indexOf(_))
     println(featInd.toString)
     df.rdd.map(x => LabeledPoint(x.getDouble(labelIndex), org.apache.spark.ml.linalg.Vectors.dense(featInd.map(x.get(_).toString.toDouble))))
  }


  def transformRawDFToLabeledPointRDDMLlib(df: Dataset[Row], sparkSession: SparkSession, labelIndex: Int,ignored: List[String]): RDD[org.apache.spark.mllib.regression.LabeledPoint] = {
    val featInd = df.columns.diff(ignored).map(df.columns.indexOf(_))
    df.rdd.map(x => org.apache.spark.mllib.regression.LabeledPoint(x.getDouble(labelIndex), org.apache.spark.mllib.linalg.Vectors.dense(featInd.map(x.get(_).toString.toDouble))))

  }

  def getSparkSession(): Unit = {
    sparkSession
  }


}
