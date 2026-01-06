package urbanpollution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingJob {

  def main(args: Array[String]): Unit = {
    val inputDir = if (args.length >= 1) args(0) else "data/stream"
    val outDir   = if (args.length >= 2) args(1) else "output/stream"

    val spark = SparkSession.builder()
      .appName("UrbanPollution-Streaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val schema = Schema.measurements

    val raw = spark.readStream
      .option("header", "true")
      .schema(schema)
      .csv(inputDir)
      .withColumn("ts", to_timestamp(col("timestamp")))
      .drop("timestamp")

    val cleaned = Analytics.clean(raw)
    val fe      = Analytics.addTimeFeatures(cleaned)

    val withPI = AnalyticsStreaming.withPollutionIndexStreaming(
      fe,
      win = "1 hour",
      watermarkDelay = "2 hours"
    )

    val withAnom = AnalyticsStreaming.withAnomalyFlagsStreaming(
      withPI,
      win = "1 hour",
      watermarkDelay = "2 hours",
      zThreshold = 3.0
    )

    val qConsole = withAnom
      .filter(col("is_anomaly") === 1)
      .select("ts", "station_id", "pm25", "pollution_index", "z_pm25")
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option("checkpointLocation", s"$outDir/_chk_console")
      .start()

    val qParquet = withAnom
      .writeStream
      .format("parquet")
      .outputMode("append")
      .option("path", s"$outDir/data")
      .option("checkpointLocation", s"$outDir/_chk_parquet")
      .start()

    spark.streams.awaitAnyTermination()
  }
}
