package offline

import java.io.FileInputStream
import java.util.Properties

import matching.lucene.analyzers.SkipGramAnalyzerWithTokenizer
import matching.ml.datamining.worldcheck.{FeatureGenerator, WorldCheckAliases}
import matching.ml.sparkapi.SparkService
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.ml.PipelineModel

import scala.io.Source

/**
  * Created by stefan on 12/9/16.
  * this is used just like test main class to test different setups, just for debugging purposes
  */
object MatchingMLMain {
  private val analyzer = new SkipGramAnalyzerWithTokenizer(1, 3)
  private val prop = new Properties()
  private val kafkaParams = Map[String, Object](
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


  def main(args: Array[String]) {
    System.out.println("Start with mining: ")
    val propertyFile = args(0)
    prop.load(new FileInputStream(propertyFile))
    val inputFile = prop.getProperty("input.file.set")
    val cleanedInputFile = prop.getProperty("input.file.cleaned")
    val outputFPFile = prop.getProperty("output.file.fp")
    val outputTPFile = prop.getProperty("output.file.tp")
    val outputBorderCases = prop.getProperty("output.file.bordercases")
    val labeledOutputFile = prop.getProperty("output.file.labeled")
    val stopWordsFile = prop.getProperty("input.stopwords")
    val featuredDataSetFile = prop.getProperty("output.file.feature.dataset")
    val names = prop.getProperty("input.file.names")
    val frequencyNameFile = prop.getProperty("output.file.frequencies")


 //   val columnNamesString = "string1;string2;ngramDistance;JaroWinklerDistance;levensteinDistance;doubleMetaphoneDistance;" +
 //      "lcsDistance;nameFrequencyDistance;label"
  /*  val columnNamesString = "string1;string2;firstNameNgramDistance;firstNameJaroWinklerDistance;firstNameMetaphoneDistance;" +
      "lastNameNgramDistance;lastNameJaroWinklerDistance;lastNameMetaphoneDistance"
      "fullNameNgramDistance;fullNameJaroWinklerDistance;fullNameMetaphoneDistance;label"; */

    val columnNamesString = "string1;string2;fullNameNgramDistance;fullNameJaroWinklerDistance;fullNameMetaphoneDistance;label";

    val similarityRatio = prop.getProperty("similarity.ratio").toDouble
    val numberOfFeautures = prop.getProperty("features.number").toInt
    val modelDestination = prop.getProperty("model.destination")
    val ignored =  prop.getProperty("dataset.column.ignored").split(",").toList
    val labelName =  prop.getProperty("dataset.column.label")
    val datasetSize  =  prop.getProperty("dataset.size").toInt
    val datasetTopic =  prop.getProperty("dataset.topic")
    val aliasesFile = prop.getProperty("dataset.aliases")
    val aliasesFileNatural = prop.getProperty("dataset.aliases.natural")

 /*   val cleaner = new matching.ml.datamining.worldcheck.RecordCleaner(aliasesFile,cleanedInputFile,stopWordsFile)
        cleaner.cleanData()
        cleaner.removeStopWords() */
    val countryStatistic = "/home/stefan/country-statistic"
    val wch1 = new WorldCheckAliases(analyzer)
//    wch1.getNaturalNames(cleanedInputFile,aliasesFileNatural)
//    wch1.removeShorAliases(aliasesFileNatural,aliasesFileNatural + "-cleaned")
    val arabicCountries = List("pakistan","afganistan","iraq","syria","jemen", "egypt", "saudi arabia",
      "iran","algeria","palestina","tunisia","jordan","oman")
    val asianCountries = List("china","japan", "thailand", "vietnam","korea, south","korea, north")
    val indianCountries = List("india")
    val eastEuropeanCountries = List("russian federation","ukraine","belarus","bulgaria","moldova")
    val latinCountries = List("brazil","argentina","spain","mexico","portugal","greece","peru","italy")
    val germanicCountries = List("united kingdom","australia","germany"," netherlands","new zealand")

    println("geting arabic names")
    wch1.filterByCitizenship(arabicCountries,aliasesFileNatural + "-cleaned",aliasesFile + "-arabic")
   println("geting asian names")
    wch1.filterByCitizenship(asianCountries,aliasesFileNatural + "-cleaned",aliasesFile + "-asian")
    println("geting indian names")
    wch1.filterByCitizenship(indianCountries,aliasesFileNatural + "-cleaned",aliasesFile + "-indian")
    println("geting east europe names")
    wch1.filterByCitizenship(eastEuropeanCountries,aliasesFileNatural + "-cleaned",aliasesFile + "-easteurope")
    println("geting latin names")
    wch1.filterByCitizenship(latinCountries,aliasesFileNatural + "-cleaned",aliasesFile + "-latin")
    println("geting germanic names")
    wch1.filterByCitizenship(germanicCountries,aliasesFileNatural + "-cleaned",aliasesFile + "-germanic")

   /* println("generating true matches arabic")
    wch1.generateSimilarTrueMatches(aliasesFile + "-arabic",outputTPFile + "-arabic",0.7)
    println("generating false matches arabic")
    wch1.generateSimilarFalseMatches(aliasesFile + "-arabic",outputFPFile + "-arabic",20000,60000)
    println("generating true matches asian")
    wch1.generateSimilarTrueMatches(aliasesFile + "-asian",outputTPFile + "-asian",0.7)
    println("generating false matches asian")
    wch1.generateSimilarFalseMatches(aliasesFile + "-asian",outputFPFile + "-asian",20000,10000)
    println("generating true matches east european")
    wch1.generateSimilarTrueMatches(aliasesFile + "-easteurope",outputTPFile + "-easteurope",0.7)
    println("generating false matches east european")
    wch1.generateSimilarFalseMatches(aliasesFile + "-easteurope",outputFPFile + "-easteurope",30000,30000) */
  //println("generating true matches indian")
 // wch1.generateSimilarTrueMatches(aliasesFile + "-indian",outputTPFile + "-indian",0.7)
  //println("generating false matches indian")
  //wch1.generateSimilarFalseMatches(aliasesFile + "-indian",outputFPFile + "-indian",20000,10000)
 // println("generating true matches latin")
//  wch1.generateSimilarTrueMatches(aliasesFile + "-latin",outputTPFile + "-latin",0.7)
 //   println("generating false matches latin")
  //  wch1.generateSimilarFalseMatches(aliasesFile + "-latin",outputFPFile + "-latin",40000,90000)
 // println("generating true matches germanic")
 // wch1.generateSimilarTrueMatches(aliasesFile + "-germanic",outputTPFile + "-germanic",0.7)
 //   println("generating false matches germanic")
 //   wch1.generateSimilarFalseMatches(aliasesFile + "-germanic",outputFPFile + "-germanic",20000,5000)

 //   wch1.generateSimilarTrueMatches(aliasesFile + "-arabic",outputTPFile + "-test-arabic",0.7)
 //   wch1.generateSimilarFalseMatches(aliasesFile + "-arabic",outputFPFile + "-test-arabic",30000,50000)

    val filePath = "/media/stefan/D27C48117C47EEB3/matching-data/aliases-to-merge"
    val mergedFilePath = "/media/stefan/D27C48117C47EEB3/matching-data/merged-aliases-pairs-arabic"
    val featuredPairsFilePath = "/media/stefan/D27C48117C47EEB3/matching-data/featured-pairs-arabic"
    // wch1.mergeTFMatches(filePath,mergedFilePath)

    val filePathTest = "/media/stefan/D27C48117C47EEB3/matching-data/aliases-to-merge-test"
    val mergedFilePathTest = "/media/stefan/D27C48117C47EEB3/matching-data/merged-aliases-pairs-arabic-test"
    val featuredPairsFilePathTest = "/media/stefan/D27C48117C47EEB3/matching-data/featured-pairs-arabic-test"
 //   wch1.mergeTFMatches(filePathTest,mergedFilePathTest)




    val columns = columnNamesString.split(";").toSeq
   // wch1.generateFeaturesFromLabeledPairs(mergedFilePath,featuredPairsFilePath,frequencyNameFile,columns,true)


  //  wch1.generateFeaturesFromLabeledPairs(mergedFilePathTest,featuredPairsFilePathTest,frequencyNameFile,columns,true)




    val service = SparkService

  //  val rawDataset = service.loadDataFromKafka(datasetTopic,Array((0,datasetSize)),kafkaParams)
    val rawDataset = service.loadDataFromCSV(featuredPairsFilePath,";",true)
  //  rawDataset.show()
    val rawDatasetTest = service.loadDataFromCSV(featuredPairsFilePathTest,";",true)
  //  rawDataset.cache()
  //  rawDatasetTest.cache()
    val trainDataset = service.transformRawDFToVectorDF(rawDataset,ignored,labelName,true)
    val trainDatasetTest = service.transformRawDFToVectorDF(rawDatasetTest,ignored,labelName,true)
  //  val weights = Array(0.8, 0.2)
   // val seed = 42
   // val datasets = service.getTrainAndTestDataDF(rawDataset,weights,seed,ignored)

    val numClasses = prop.getProperty("randomforest.classes.number").toInt
    val numTrees = prop.getProperty("randomforest.trees.number").toInt
    val featureSubsetStrategy = prop.getProperty("randomforest.fs.strategy")
    val impurity = prop.getProperty("randomforest.impurity")
    val maxDepth = prop.getProperty("randomforest.depth.max").toInt
    val maxBins = prop.getProperty("randomforest.bins.max").toInt
    val maxMemory = prop.getProperty("randomforest.memory.max").toInt

    val maxIter = prop.getProperty("logregression.iter.max").toInt
    val regParam = prop.getProperty("logregression.regparam").toDouble
    val elesticNetParam = prop.getProperty("logregression.elasticnetparam").toDouble
    val treshold = prop.getProperty("logregression.treshold").toDouble
    val family = "binomial"

   //  val pipelineArray = Array(service.buildRandomForest(numClasses, numTrees, maxBins, maxDepth, featureSubsetStrategy, impurity, maxMemory))
    val pipelineArray = Array(service.buildLogisticRegression(maxIter,regParam,elesticNetParam,family,treshold))



    // testDataset = service.transformRawDFToLabeledPointRDDMLlib(rawDatasetTest, labelName::ignored)
   // val svm = service.buildSVMachines(10000,0.01,10.0,0.1)
  //  val model = svm.run(trainDataset)
  /*  val model = service.buildModel(trainDataset,pipelineArray)
    model.save(modelDestination)
    val writer = new PrintWriter(prop.getProperty("output.file.result"))
    val result = service.evaluateTestDataML(trainDatasetTest,model)
    println("resulting accuracy: " + result)
    writer.write("resulting accuracy:" + result + "\n")
    writer.close() */



  //  val paramGrid = new ParamGridBuilder().build()
  //  val model = service.crossValidate(trainDataset,paramGrid,pipelineArray,3);
   // val model = service.buildModel(trainDataset, pipelineArray)
    // model.save(modelDestination)

    val model = PipelineModel.load(modelDestination)
    //val aliasesToMatchFile = "/home/stefan/sample"
    val aliasesToMatchFile = "/media/stefan/D27C48117C47EEB3/matching-data/all-aliases-arabic"


    val rawFeaturesGenerated = FeatureGenerator.generateFeatureForMatchAgainstFile("alahmad,mahmud dhiyab","iraq",aliasesToMatchFile,";")
    val featuredDS = SparkService.transformRawDFToVectorDF(rawFeaturesGenerated,ignored,labelName,false)
    println("success")

    val modeApplied = model.transform(featuredDS)
 /*   val jaroWinklerFunc : (String,String) => Double = (s1,s2) => {
      val jaroWinkler = new JaroWinklerStringDistance
      jaroWinkler.getDistance(s1,s2)
    }
    import org.apache.spark.sql.functions.udf
    val jaroWinklerFuncUDF = udf(jaroWinklerFunc) */


    val cols = modeApplied.columns
    cols.foreach(println)
    val results = modeApplied
      .filter(x => x.getDouble(5) == 1.0)
    val pairsNumber = featuredDS.count()
    val matchesNumber = results.count()
    results.show(100,false)
    //println("resulting accuracy: " + results)
    println("Number of pairs: " + pairsNumber);
    println("Number of matches: " + matchesNumber);



  /*  System.out.println("Start streaming job")


    val aliasesToMatchFile = prop.getProperty("dataset.aliases")
    println(aliasesToMatchFile)
    val streamTopic = prop.getProperty("stream.topic")



    val fg = new FeatureGenerator(frequencyNameFile)

    val streamDSRaw = service.createKafkaStreamStringDataSet(streamTopic,kafkaParams)
    //val streamFeaturedPairsDS = streamDSRaw.map(x => service.transformRawDFToVectorDF(fg.generateFeatureFromStringAgainstFile(x,aliasesToMatchFile,";"),ignored));

    //val model = PipelineModel.load(modelDestination)
   // val predictions = streamFeaturedPairsDS.map(x => model.transform(x).select("prediction"))
    val query = streamDSRaw.writeStream.foreach(new ForeachWriter[String] {
     override def open(partitionId: Long, version: Long): Boolean = {
       println("open("+partitionId+","+version+")")
       true
     }

     override def process(record: String) : Unit = {
       println("process string : " +record)
       println("aliases dataset is: " + aliasesToMatchFile)
       println("ignored: " + ignored)
       val featuredDS = SparkService.transformRawDFToVectorDF(
         fg.generateFeatureFromStringAgainstFile(record,aliasesToMatchFile,";"),ignored,false)
       println("success")
       featuredDS.printSchema()
       featuredDS.show()
       val modeApplied = model.transform(featuredDS)
       val results = modeApplied.select("string1","string2","prediction").filter(x => x.getDouble(2) == 1.0)
       println("Number of pairs: " + featuredDS.count());
       println("Number of matches: " + results.count());

       results.show(10)

     }

     override def close(errorOrNull: Throwable): Unit = {
       // close the connection
       println("close()")
     }
   }).start()
    query.awaitTermination() */
















    //   val cleaner = new matching.ml.datamining.worldcheck.RecordCleaner(inputFile,cleanedInputFile,stopWordsFile)
    //    cleaner.cleanData()
    //    cleaner.removeStopWords()
    //   val wch = new WorldCheckAliases(analyzer, cleanedInputFile, outputFPFile, outputTPFile, similarityRatio)
    // wch.generateNameFrequencyList(names, frequencyNameFile)
    //   wch.generateBorderCases(outputTPFile,outputBorderCases)

    //  wch.generateSimilarFalseMatches
    //  wch.generateSimilarTrueMatches
    //   wch.generateLabels(labeledOutputFile,true)
   //  val service = new SparkService
   //  val fg = new FeatureGenerator(frequencyNameFile,service.getSparkSession)
  //   fg.generateFeatureFromStringAgainstFile("Mohammed Sani ABACHA",inputFile,labeledOutputFile)
    //  fg.generateFeatures(featuredDataSetFile,frequencyNameFile)

/*    val service = new SparkService
    val evaluator = new AlgorithmEvaluator(service)
    val ignored = List("string1", "string2", "label")
    val weights = Array(0.9, 0.1)
    val seed = 42
    val topic = "test"
  //  val offsets = Array((0,460095))
  //  val offsets = Array((0,1000))
    val labelName = "label"
 //   val rawDataset = service.loadDataFromKafka(topic,offsets,kafkaParams)
    val rawDataset = service.loadDataFromCSV(featuredDataSetFile)
    rawDataset.cache()
    val featuresNames = rawDataset.columns.filter(x => !ignored.contains(x))
    val datasets = service.getTrainAndTestDataDF(rawDataset,weights,seed,ignored,labelName)






    //cros-validation setup
    //   val pipeline = new Pipeline().setStages(Array(service.buildRandomForest()))
    //   val paramGrid = new ParamGridBuilder().build()
    //   val cvModel = service.crossValidate(fullDataset.toDF("label","features"),paramGrid,pipeline,3)
    //   val RFresult2 = service.evaluateTestDataML(datasets._2,cvModel)


    val writer = new PrintWriter(prop.getProperty("output.file.result"))

   // var model = service.loadModel(modelDestination)


      val numClasses = prop.getProperty("randomforest.classes.number").toInt
      val numTrees = prop.getProperty("randomforest.trees.number").toInt
      val featureSubsetStrategy = prop.getProperty("randomforest.fs.strategy")
      val impurity = prop.getProperty("randomforest.impurity")
      val maxDepth = prop.getProperty("randomforest.depth.max").toInt
      val maxBins = prop.getProperty("randomforest.bins.max").toInt
      val maxMemory = prop.getProperty("randomforest.memory.max").toInt


      val pipelineArray = Array(service.buildRandomForest(numClasses, numTrees, maxBins, maxDepth, featureSubsetStrategy, impurity, maxMemory))
     // val model = service.buildModel(datasets._1, pipelineArray)
     // model.save(modelDestination)

    val result = evaluator.evaluateAlgorithmSetup(datasets,pipelineArray)
    println("resulting accuracy: " + result)
    writer.write("resulting accuracy:" + result + "\n")



/*    val streamTopic = "streamTopic"
    val columnNames = rawDataset.columns;
    val schema = StructType(columnNames.map(x => StructField(x,StringType)))
    columnNames.foreach(println)

    val streamDSRaw = service.createKafkaStreamDataSet(streamTopic,kafkaParams,columnNames,schema)
    val streamDS = service.transformRawDFToVectorDF(streamDSRaw,labelName,ignored)

    val predictions = model.transform(streamDS).select("prediction")
    val query = predictions.writeStream.format("console").start()
    query.awaitTermination() */

    //predictions.show()

 /*   val result = evaluator.evaluateAlgorithmSetup(datasets,pipelineArray)
    println("resulting accuracy: " + result)
    writer.write("resulting accuracy:" + result + "\n")

    println("evaluate particular feautures: ")
    writer.write("evaluate particular feautures:\n") */

  /*  featuresNames.foreach(x => {
      val result = evaluator.evaluateFeature(datasets, pipelineArray, featuresNames.indexOf(x))
      writer.write("Feature " + x + "accuracy is: " + result + "\n")
      println("Feature " + x + "accuracy is: " + result)
    })

    featuresNames.foreach(x => {
      val result = evaluator.evaluateAgainstFeature(datasets, pipelineArray, featuresNames.indexOf(x), numberOfFeautures)
      writer.write("All against Feature " + x + "accuracy is: " + result + "\n")
      println("All against Feature " + x + "accuracy is: " + result)
    })
    writer close() */
    */

    /*  val columnsToDiscretize = "ngramDistance;JaroWinklerDistance;levensteinDistance;doubleMetaphoneDistance;lcsDistance".split(";").toSeq

    val splits = Array(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, Double.PositiveInfinity)

    var discretizerDF = rawDataset
    columnsToDiscretize.foreach(x=> discretizerDF = service.bucketizeColumn(discretizerDF,splits,x,"bucket_" + x).drop(x).withColumnRenamed("bucket_" + x,x))

    rawDataset.filter(x => x.getDouble(2) > 1.0).show() */

    /*  discretizerDF = service.discretizeColumn(discretizerDF,"nameFrequencyDistance","discretized_nameFrequencyDistance",10)
        .drop("nameFrequencyDistance")
        .withColumnRenamed("discretized_nameFrequencyDistance","nameFrequencyDistance") */

    //  discretizerDF = discretizerDF.drop("nameFrequencyDistance").filter(x => x.getDouble(6) > 1.0)
    // discretizerDF.show()


    /* var discretizerDF = rawDataset
     columnsToDiscretize.foreach(x=> discretizerDF = service.discretizeColumn(discretizerDF,x,"discretized_" + x))

     discretizerDF.show(5,false) */

    // val trainDF = service.transformRawDFToVectorDF(discretizerDF,ignored,true).filter(x => x.getDouble(6) > 1.0)
    //  trainDF.show()

    /* val selector = new InfoThSelector()
       .setSelectCriterion("mrmr")
       .setNPartitions(100)
       .setNumTopFeatures(5)
       .setFeaturesCol("features")
       .setLabelCol("label")
       .setOutputCol("selectedFeatures")

     val result = selector.fit(trainDF).transform(trainDF)
     result.show() */

  }

  def firstLine(f: java.io.File): Option[String] = {
    val src = Source.fromFile(f)
    try {
      src.getLines.find(_ => true)
    } finally {
      src.close()
    }
  }


}
