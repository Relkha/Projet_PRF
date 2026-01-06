package urbanpollution

object App {
  // Point d’entrée "humain" : indique comment lancer
  def main(args: Array[String]): Unit = {
    println(
      """Urban Pollution Spark project
        |Try:
        |  sbt "runMain urbanpollution.BatchJob data output"
        |  sbt "runMain urbanpollution.StreamingJob data/stream output/stream"
        |  sbt "runMain urbanpollution.DataGenerator data 30 21"
        |""".stripMargin
    )
  }
}
