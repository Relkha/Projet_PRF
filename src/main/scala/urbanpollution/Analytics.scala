package urbanpollution

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Analytics {

  /** Nettoyage demandé : suppression doublons + valeurs manquantes. */
  def clean(meas: DataFrame): DataFrame = {
    meas
      .dropDuplicates("ts","station_id")
      .na.drop(Seq("ts","station_id","pm25"))
      .na.fill(Map(
        "co2" -> 420.0,
        "noise_db" -> 40.0,
        "humidity" -> 50.0,
        "temp_c" -> 15.0,
        "passenger_traffic" -> 0,
        "is_holiday" -> 0,
        "event_level" -> 0
      ))
  }

  /** Extraction variables temporelles (heure, jour, mois) demandée. */
  def addTimeFeatures(df: DataFrame): DataFrame = {
    df
      .withColumn("hour", hour(col("ts")))
      .withColumn("day_of_week", (weekday(col("ts")) + lit(1)).cast("int")) // 1..7
      .withColumn("month", month(col("ts")))
      .withColumn("is_peak_hour",
        when(col("hour").between(7,9) || col("hour").between(17,19), 1).otherwise(0)
      )
  }

  /** Stats par station / par ligne : moyenne, min, max + p95. */
  def stationStats(df: DataFrame): DataFrame =
    df.groupBy("station_id")
      .agg(
        avg("pm25").as("pm25_mean"),
        min("pm25").as("pm25_min"),
        max("pm25").as("pm25_max"),
        expr("percentile_approx(pm25, 0.95)").as("pm25_p95"),
        avg("co2").as("co2_mean"),
        avg("noise_db").as("noise_mean")
      )
      .orderBy(desc("pm25_mean"))

  def lineStats(df: DataFrame): DataFrame =
    df.groupBy("line_id")
      .agg(
        avg("pm25").as("pm25_mean"),
        max("pm25").as("pm25_max"),
        avg("passenger_traffic").as("traffic_mean")
      )
      .orderBy(desc("pm25_mean"))

  /** Pics horaires : station x hour. */
  def hourlyPeaks(df: DataFrame): DataFrame =
    df.groupBy("station_id","hour")
      .agg(avg("pm25").as("pm25_mean_hour"))
      .orderBy(desc("pm25_mean_hour"))

  /**
   * Indicateur global de pollution (0..1) = moyenne pondérée de min-max normalisés
   * (par station) sur CO2, PM2.5, bruit, humidité.
   */
  def withPollutionIndex(df: DataFrame): DataFrame = {
    val w = Window.partitionBy("station_id")

    def minMax(colName: String) = {
      val c = col(colName)
      val cmin = min(c).over(w)
      val cmax = max(c).over(w)
      when(cmax === cmin, lit(0.0)).otherwise((c - cmin) / (cmax - cmin))
    }

    val co2n = minMax("co2")
    val pmn  = minMax("pm25")
    val noisen = minMax("noise_db")
    val humn = minMax("humidity")

    df.withColumn("co2_n", co2n)
      .withColumn("pm25_n", pmn)
      .withColumn("noise_n", noisen)
      .withColumn("humidity_n", humn)
      .withColumn("pollution_index",
        col("pm25_n")*0.45 + col("co2_n")*0.35 + col("noise_n")*0.15 + col("humidity_n")*0.05
      )
  }

  /** Anomalies : Z-score (par station) sur PM2.5 + indice global. */
  def withAnomalies(df: DataFrame): DataFrame = {
    val w = Window.partitionBy("station_id")
    val meanPm = avg(col("pm25")).over(w)
    val stdPm  = stddev_pop(col("pm25")).over(w)
    val meanPI = avg(col("pollution_index")).over(w)
    val stdPI  = stddev_pop(col("pollution_index")).over(w)

    val zPm = (col("pm25") - meanPm) / when(stdPm === 0.0, lit(1.0)).otherwise(stdPm)
    val zPI = (col("pollution_index") - meanPI) / when(stdPI === 0.0, lit(1.0)).otherwise(stdPI)

    df.withColumn("z_pm25", zPm)
      .withColumn("z_pi", zPI)
      .withColumn("is_anomaly",
        when(abs(col("z_pm25")) > 3.0 || abs(col("z_pi")) > 3.0, 1).otherwise(0)
      )
  }

  /**
   * Démonstration "programmation fonctionnelle" (RDD):
   * - map/filter/flatMap/reduceByKey pour compter anomalies par station.
   */
  def anomalyCountsRDD(spark: SparkSession, anomalies: DataFrame): DataFrame = {
    import spark.implicits._
    val rdd = anomalies.select("station_id","is_anomaly").rdd

    val counts = rdd
      .map(row => (row.getString(0), row.getInt(1)))            // map
      .filter { case (_, isA) => isA == 1 }                    // filter
      .flatMap { case (sid, _) => Seq((sid, 1)) }              // flatMap (trivial mais explicite)
      .reduceByKey(_ + _)                                      // reduce
      .toDF("station_id","anomaly_count")

    counts
  }
}
