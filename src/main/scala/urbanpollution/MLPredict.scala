package urbanpollution

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.storage.StorageLevel

object MLPredict {

  /**
   * Prépare un dataset "t -> t+1h" par station (lag/lead).
   * label = pm25_next
   */
  def makeSupervised(df: DataFrame): DataFrame = {
    val w = Window.partitionBy("station_id").orderBy(col("ts"))

    df.withColumn("pm25_next", lead(col("pm25"), 1).over(w))
      .withColumn("pi_next", lead(col("pollution_index"), 1).over(w))
      .withColumn("pm25_lag1", lag(col("pm25"), 1).over(w))
      .withColumn("pi_lag1", lag(col("pollution_index"), 1).over(w))
      .na.drop(Seq("pm25_next","pm25_lag1","pi_lag1"))
  }

  def trainAndEval(spark: SparkSession, supervised: DataFrame, outPath: String): Unit = {

    spark.conf.set("spark.sql.shuffle.partitions", "64")

    // 1) Base
    val base = supervised.select(
      col("pm25_next").cast("double").as("label"),
      col("hour").cast("double"),
      col("day_of_week").cast("double"),
      col("month").cast("double"),
      col("passenger_traffic").cast("double"),
      col("temp_c").cast("double"),
      col("humidity").cast("double"),
      col("is_peak_hour").cast("double"),
      col("event_level").cast("double"),
      col("is_holiday").cast("double"),
      col("pm25_lag1").cast("double"),
      col("pi_lag1").cast("double"),
      col("station_id").cast("string")
    ).persist(StorageLevel.MEMORY_AND_DISK)

    val Array(train0, test0) = base.randomSplit(Array(0.8, 0.2), seed = 42)
    val train = train0.persist(StorageLevel.MEMORY_AND_DISK)
    val test  = test0.persist(StorageLevel.MEMORY_AND_DISK)

    val idx = new StringIndexer()
      .setInputCol("station_id")
      .setOutputCol("station_idx")
      .setHandleInvalid("keep")

    val ohe = new OneHotEncoder()
      .setInputCol("station_idx")
      .setOutputCol("station_ohe")
      .setDropLast(true)


    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "hour","day_of_week","month",
        "passenger_traffic","temp_c","humidity",
        "is_peak_hour","event_level","is_holiday",
        "pm25_lag1","pi_lag1",
        "station_ohe"
      ))
      .setOutputCol("features")


    val rf = new RandomForestRegressor()
      .setNumTrees(15)
      .setMaxDepth(6)
      .setMaxBins(64)
      .setSubsamplingRate(0.7)
      .setFeatureSubsetStrategy("sqrt")
      .setMinInstancesPerNode(5)
      .setMaxMemoryInMB(256)

    val gbt = new GBTRegressor()
      .setMaxIter(40)
      .setMaxDepth(5)
      .setMaxBins(64)
      .setStepSize(0.1)
      .setSubsamplingRate(0.7)
      .setMaxMemoryInMB(256)

    val pipelineRF  = new Pipeline().setStages(Array(idx, ohe, assembler, rf))
    val pipelineGBT = new Pipeline().setStages(Array(idx, ohe, assembler, gbt))

    val evalRMSE = new RegressionEvaluator().setMetricName("rmse")
    val evalMAE  = new RegressionEvaluator().setMetricName("mae")
    val evalR2   = new RegressionEvaluator().setMetricName("r2")


    val rfModel = pipelineRF.fit(train)
    val rfPred  = rfModel.transform(test)

    val gbtModel = pipelineGBT.fit(train)
    val gbtPred  = gbtModel.transform(test)

    val metrics = spark.createDataFrame(Seq(
      ("RandomForest", evalRMSE.evaluate(rfPred),  evalMAE.evaluate(rfPred),  evalR2.evaluate(rfPred)),
      ("GBT",          evalRMSE.evaluate(gbtPred), evalMAE.evaluate(gbtPred), evalR2.evaluate(gbtPred))
    )).toDF("model","rmse","mae","r2")


    metrics.coalesce(1)
      .write.mode("overwrite").option("header","true")
      .csv(s"$outPath/metrics")


    rfPred.select("label","prediction","station_id","hour","pm25_lag1","passenger_traffic")
      .repartition(8)
      .write.mode("overwrite").option("header","true")
      .csv(s"$outPath/rf_predictions")

    gbtPred.select("label","prediction","station_id","hour","pm25_lag1","passenger_traffic")
      .repartition(8)
      .write.mode("overwrite").option("header","true")
      .csv(s"$outPath/gbt_predictions")

    train.unpersist()
    test.unpersist()
    base.unpersist()
  }
}
