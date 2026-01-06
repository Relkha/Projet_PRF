package urbanpollution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Traitement en temps réel (optionnel) :
 * - readStream (CSV) depuis un dossier (simulation capteurs)
 * - transformations fonctionnelles
 * - détection anomalies (Z-score approx sur fenêtre glissante simplifiée)
 */
object StreamingJob {

  def main(args: Array[String]): Unit = {
    val inputDir = if (args.length >= 1) args(0) else "data/stream"
    val outDir   = if (args.length >= 2) args(1) else "output/stream"

    val spark = SparkSession.builder()
      .appName("UrbanPollution-Streaming")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val schema = Schema.measurements

    val raw = spark.readStream
      .option("header","true")
      .schema(schema)
      .csv(inputDir)
      .withColumn("ts", to_timestamp(col("timestamp")))
      .drop("timestamp")

    val cleaned = Analytics.clean(raw)
    val fe = Analytics.addTimeFeatures(cleaned)
    val withPI = Analytics.withPollutionIndex(fe)

    // Z-score sur fenêtre glissante (1h) : approx simple
    val win = org.apache.spark.sql.expressions.Window
      .partitionBy("station_id")
      .orderBy(col("ts"))
      .rowsBetween(-60, 0)  // ~ 60 lignes si 1/min; à ajuster selon ton flux

    val meanPm = avg(col("pm25")).over(win)
    val stdPm  = stddev_pop(col("pm25")).over(win)

    val withAnom = withPI
      .withColumn("z_pm25", (col("pm25") - meanPm) / when(stdPm === 0.0, lit(1.0)).otherwise(stdPm))
      .withColumn("is_anomaly", when(abs(col("z_pm25")) > 3.0, 1).otherwise(0))

    val q = withAnom
      .filter(col("is_anomaly") === 1)
      .select("ts","station_id","pm25","pollution_index","z_pm25")
      .writeStream
      .format("console")
      .option("truncate","false")
      .option("checkpointLocation", s"$outDir/_chk_console")
      .start()

    val q2 = withAnom
      .writeStream
      .format("parquet")
      .option("path", s"$outDir/data")
      .option("checkpointLocation", s"$outDir/_chk_parquet")
      .outputMode("append")
      .start()

    q.awaitTermination()
    q2.awaitTermination()
  }
}
