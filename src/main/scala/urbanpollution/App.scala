package urbanpollution

object App {
  // Point d’entrée "humain" : indique comment lancer
  def main(args: Array[String]): Unit = {
    println(
      """Urban Pollution Spark project
        |Try:
        |  spark-submit \
          |  --class urbanpollution.StreamingJob \
          |  --master "local[*]" \
          |  target/scala-2.12/urban-pollution-spark_2.12-0.1.0.jar \
          |  data/stream output/stream

          spark-submit \
            --class urbanpollution.BatchJob \
            --master "local[*]" \
            target/scala-2.12/urban-pollution-spark_2.12-0.1.0.jar \
            data output
            
        |  sbt "runMain urbanpollution.DataGenerator data 30 21"
        |""".stripMargin
    )
  }
}
