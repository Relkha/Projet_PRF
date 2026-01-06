package urbanpollution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Batch pipeline (phases 1..5) :
 * - ingestion + nettoyage
 * - transformations fonctionnelles + exploration
 * - analyses (indice, anomalies)
 * - graph (GraphX) + propagation simple
 * - prédiction (MLlib)
 */
object BatchJob {

  def main(args: Array[String]): Unit = {
    val dataDir = if (args.length >= 1) args(0) else "data"
    val outDir  = if (args.length >= 2) args(1) else "output"

    val spark = SparkSession.builder()
      .appName("UrbanPollution-Batch")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val measPath = s"$dataDir/measurements.csv"
    val stationsPath = s"$dataDir/stations.csv"
    val edgesPath = s"$dataDir/edges.csv"

    val raw = IO.readMeasurements(spark, measPath)
    val stations = IO.readStations(spark, stationsPath)
    val edges = IO.readEdges(spark, edgesPath)

    // Phase 1 : nettoyage
    val cleaned = Analytics.clean(raw)

    // Phase 2 : features temporelles
    val fe = Analytics.addTimeFeatures(cleaned)

    // Phase 3 : analyses (indice global + anomalies)
    val withPI = Analytics.withPollutionIndex(fe)
    val withAnom = Analytics.withAnomalies(withPI).cache()

    // Stats
    val stats = Analytics.stationStats(withAnom)
    stats.coalesce(1).write.mode("overwrite").option("header","true").csv(s"$outDir/station_stats")

    val peaks = Analytics.hourlyPeaks(withAnom).limit(200)
    peaks.coalesce(1).write.mode("overwrite").option("header","true").csv(s"$outDir/peaks")

    // Anomalies
    withAnom.filter(col("is_anomaly") === 1)
      .select("ts","station_id","pm25","pollution_index","z_pm25","z_pi")
      .orderBy(desc("z_pm25"))
      .coalesce(1).write.mode("overwrite").option("header","true").csv(s"$outDir/anomalies")

    // Démo RDD (map/filter/flatMap/reduceByKey) : nb anomalies par station
    val anomCounts = Analytics.anomalyCountsRDD(spark, withAnom)
    anomCounts.coalesce(1).write.mode("overwrite").option("header","true").csv(s"$outDir/anomaly_counts_rdd")

    // Phase 4 : GraphX (centralité)
    val metrics = GraphJob.graphMetrics(spark, stations, edges)
    metrics.coalesce(1).write.mode("overwrite").option("header","true").csv(s"$outDir/graph_metrics")

    // Propagation simple sur un instant t (on prend le dernier timestamp)
    val lastTs = withAnom.agg(max(col("ts")).as("last_ts")).collect()(0).getAs[java.sql.Timestamp]("last_ts")
    val piAtLast = withAnom.filter(col("ts") === lit(lastTs)).select("station_id","pollution_index")
    val propagated = GraphJob.propagatePollution(spark, stations, edges, piAtLast, alpha = 0.7, k = 5)
    propagated.coalesce(1).write.mode("overwrite").option("header","true").csv(s"$outDir/propagation")

    // Phase 5 : Prédiction
    val supervised = MLPredict.makeSupervised(withAnom)
    MLPredict.trainAndEval(spark, supervised, s"$outDir/predictions")

    println(s"[OK] Batch terminé. Résultats dans: $outDir")
    spark.stop()
  }
}
