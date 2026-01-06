package urbanpollution

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Schema {
  val measurements: StructType = new StructType()
    .add("timestamp", StringType, nullable = false)
    .add("station_id", StringType, nullable = false)
    .add("line_id", StringType, nullable = true)
    .add("co2", DoubleType, nullable = true)              // ppm
    .add("pm25", DoubleType, nullable = true)             // µg/m3
    .add("noise_db", DoubleType, nullable = true)         // dB
    .add("humidity", DoubleType, nullable = true)         // %
    .add("temp_c", DoubleType, nullable = true)           // °C
    .add("passenger_traffic", IntegerType, nullable = true)
    .add("is_holiday", IntegerType, nullable = true)
    .add("event_level", IntegerType, nullable = true)

  val stations: StructType = new StructType()
    .add("station_id", StringType, nullable = false)
    .add("name", StringType, nullable = false)
    .add("lat", DoubleType, nullable = false)
    .add("lon", DoubleType, nullable = false)
    .add("zone", StringType, nullable = false)
    .add("transport_type", StringType, nullable = false)

  val edges: StructType = new StructType()
    .add("src_station_id", StringType, nullable = false)
    .add("dst_station_id", StringType, nullable = false)
    .add("distance_m", DoubleType, nullable = false)
    .add("line_id", StringType, nullable = true)
}

object IO {
  def readMeasurements(spark: SparkSession, path: String): DataFrame = {
    spark.read.option("header","true").schema(Schema.measurements).csv(path)
      .withColumn("ts", to_timestamp(col("timestamp")))
      .drop("timestamp")
  }

  def readStations(spark: SparkSession, path: String): DataFrame =
    spark.read.option("header","true").schema(Schema.stations).csv(path)

  def readEdges(spark: SparkSession, path: String): DataFrame =
    spark.read.option("header","true").schema(Schema.edges).csv(path)
}
