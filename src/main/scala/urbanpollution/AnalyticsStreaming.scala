package urbanpollution

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Helpers STREAM-SAFE (pas de .over Window SQL).
 * On fait :
 * - watermark sur ts
 * - colonne win = window(ts, winDuration)
 * - agrégation par (station_id, win)
 * - join INNER sur (station_id, win)
 */
object AnalyticsStreaming {

  def withPollutionIndexStreaming(
      df: DataFrame,
      win: String = "1 hour",
      watermarkDelay: String = "2 hours"
  ): DataFrame = {

    val s = df
      .withWatermark("ts", watermarkDelay)
      .withColumn("win", window(col("ts"), win))

    val stats = s
      .groupBy(col("station_id"), col("win"))
      .agg(
        min(col("co2")).as("co2_min"),
        max(col("co2")).as("co2_max"),
        min(col("pm25")).as("pm25_min"),
        max(col("pm25")).as("pm25_max")
      )

    val j = s.join(stats, Seq("station_id", "win"), "inner")

    val co2Den = when(col("co2_max").isNull || col("co2_min").isNull || col("co2_max") === col("co2_min"), lit(1.0))
      .otherwise(col("co2_max") - col("co2_min"))

    val pmDen = when(col("pm25_max").isNull || col("pm25_min").isNull || col("pm25_max") === col("pm25_min"), lit(1.0))
      .otherwise(col("pm25_max") - col("pm25_min"))

    val co2Norm = (col("co2") - col("co2_min")) / co2Den
    val pmNorm  = (col("pm25") - col("pm25_min")) / pmDen

    j.withColumn("pollution_index", (co2Norm + pmNorm) / lit(2.0))
      .drop("co2_min", "co2_max", "pm25_min", "pm25_max")
  }

  def withAnomalyFlagsStreaming(
      dfWithPI: DataFrame,
      win: String = "1 hour",
      watermarkDelay: String = "2 hours",
      zThreshold: Double = 3.0
  ): DataFrame = {

    val s = dfWithPI
      .withWatermark("ts", watermarkDelay)
      .withColumn("win", window(col("ts"), win))

    val stats = s
      .groupBy(col("station_id"), col("win"))
      .agg(
        avg(col("pm25")).as("pm25_mean"),
        stddev_pop(col("pm25")).as("pm25_std")
      )

    val j = s.join(stats, Seq("station_id", "win"), "inner")

    val stdSafe = when(col("pm25_std").isNull || col("pm25_std") === 0.0, lit(1.0)).otherwise(col("pm25_std"))

    j.withColumn("z_pm25", (col("pm25") - col("pm25_mean")) / stdSafe)
      .withColumn("is_anomaly", when(abs(col("z_pm25")) > lit(zThreshold), 1).otherwise(0))
      .drop("pm25_mean", "pm25_std")
  }
}
