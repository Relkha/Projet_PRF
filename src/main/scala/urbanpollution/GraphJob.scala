package urbanpollution

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object GraphJob {

  /**
   * Construit le graphe stations-connexions (GraphX) et calcule :
   * - degré (centralité simple)
   * - PageRank
   */
  def graphMetrics(spark: SparkSession, stations: DataFrame, edges: DataFrame): DataFrame = {
    import spark.implicits._

    // VertexId stable via zipWithIndex
    val vertices: RDD[(VertexId, String)] =
      stations.select("station_id").as[String].rdd.zipWithIndex.map { case (sid, idx) =>
        (idx, sid)
      }

    val idMap = vertices.map { case (vid, sid) => (sid, vid) }.collectAsMap()
    val bc = spark.sparkContext.broadcast(idMap)

    val graphEdges: RDD[Edge[Double]] = edges.select("src_station_id","dst_station_id","distance_m").rdd.flatMap { r =>
      val src = r.getString(0); val dst = r.getString(1); val dist = r.getDouble(2)
      val m = bc.value
      if (m.contains(src) && m.contains(dst)) Some(Edge(m(src), m(dst), dist)) else None
    }

    val g = Graph(vertices, graphEdges)

    val deg = g.degrees.map { case (vid, d) => (vid, d.toDouble) }.toDF("vid","degree")
    val pr  = g.pageRank(0.0001).vertices.toDF("vid","pagerank")

    val vdf = vertices.toDF("vid","station_id")

    vdf.join(deg, "vid").join(pr, "vid")
      .select("station_id","degree","pagerank")
      .orderBy(desc("pagerank"))
  }

  /**
   * Propagation simple de la pollution : à chaque pas,
   * PI_new(station) = alpha*PI_old(station) + (1-alpha)*moyenne(PI_old(voisins))
   * On renvoie un DataFrame (station_id, pi_after_k)
   */
  def propagatePollution(
      spark: SparkSession,
      stations: DataFrame,
      edges: DataFrame,
      pollutionAtT: DataFrame,
      alpha: Double,
      k: Int
  ): DataFrame = {
    import spark.implicits._

    val vertices: RDD[(VertexId, String)] =
      stations.select("station_id").as[String].rdd.zipWithIndex.map { case (sid, idx) =>
        (idx, sid)
      }

    val idMap = vertices.map { case (vid, sid) => (sid, vid) }.collectAsMap()
    val bc = spark.sparkContext.broadcast(idMap)

    val graphEdges: RDD[Edge[Int]] = edges.select("src_station_id","dst_station_id").rdd.flatMap { r =>
      val src = r.getString(0); val dst = r.getString(1)
      val m = bc.value
      if (m.contains(src) && m.contains(dst)) Some(Edge(m(src), m(dst), 1)) else None
    }

    // init PI (si station absente -> 0)
    val piMap = pollutionAtT.select("station_id","pollution_index").as[(String, Double)].collect().toMap
    val bcPI = spark.sparkContext.broadcast(piMap)

    val initial: RDD[(VertexId, Double)] =
      vertices.map { case (vid, sid) => (vid, bcPI.value.getOrElse(sid, 0.0)) }

    var g = Graph(initial, graphEdges)

    // Pregel diffusion
    val initialMsg = 0.0
    val g2 = Pregel(g, initialMsg, k)(
      vprog = (id, pi, msg) => alpha*pi + (1.0-alpha)*msg,
      sendMsg = triplet => {
        val msgToDst = triplet.srcAttr
        val msgToSrc = triplet.dstAttr
        Iterator((triplet.dstId, msgToDst), (triplet.srcId, msgToSrc))
      },
      mergeMsg = (a, b) => (a + b) / 2.0
    )

    val out = g2.vertices.toDF("vid","pi_after")
    val vdf = vertices.toDF("vid","station_id")
    vdf.join(out, "vid").select("station_id","pi_after").orderBy(desc("pi_after"))
  }
}
