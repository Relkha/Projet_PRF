package urbanpollution

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, OneHotEncoder}
import org.apache.spark.ml.regression.{GBTRegressor, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator

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
    // Features: temps + contexte + lag
    val base = supervised.select(
      col("pm25_next").as("label"),
      col("hour"), col("day_of_week"), col("month"),
      col("passenger_traffic"), col("temp_c"), col("humidity"),
      col("is_peak_hour"), col("event_level"), col("is_holiday"),
      col("pm25_lag1"), col("pi_lag1"),
      col("station_id")
    )

    // Encoder station_id (catégorie)
    val idx = new StringIndexer().setInputCol("station_id").setOutputCol("station_idx").setHandleInvalid("keep")
    val ohe = new OneHotEncoder().setInputCol("station_idx").setOutputCol("station_ohe")

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
      .setNumTrees(120)
      .setMaxDepth(10)

    val gbt = new GBTRegressor()
      .setMaxIter(80)
      .setMaxDepth(6)

    val pipelineRF = new Pipeline().setStages(Array(idx, ohe, assembler, rf))
    val pipelineGBT = new Pipeline().setStages(Array(idx, ohe, assembler, gbt))

    val Array(train, test) = base.randomSplit(Array(0.8, 0.2), seed = 42)

    val evalRMSE = new RegressionEvaluator().setMetricName("rmse")
    val evalMAE  = new RegressionEvaluator().setMetricName("mae")
    val evalR2   = new RegressionEvaluator().setMetricName("r2")

    val rfModel = pipelineRF.fit(train)
    val rfPred = rfModel.transform(test)

    val gbtModel = pipelineGBT.fit(train)
    val gbtPred = gbtModel.transform(test)

    val metrics = spark.createDataFrame(Seq(
      ("RandomForest", evalRMSE.evaluate(rfPred), evalMAE.evaluate(rfPred), evalR2.evaluate(rfPred)),
      ("GBT",          evalRMSE.evaluate(gbtPred), evalMAE.evaluate(gbtPred), evalR2.evaluate(gbtPred))
    )).toDF("model","rmse","mae","r2")

    metrics.coalesce(1).write.mode("overwrite").option("header","true").csv(s"$outPath/metrics")

    // Sauvegarde quelques prédictions
    rfPred.select("label","prediction","station_id","hour","pm25_lag1","passenger_traffic")
      .coalesce(1).write.mode("overwrite").option("header","true").csv(s"$outPath/rf_predictions")

    gbtPred.select("label","prediction","station_id","hour","pm25_lag1","passenger_traffic")
      .coalesce(1).write.mode("overwrite").option("header","true").csv(s"$outPath/gbt_predictions")
  }
}
