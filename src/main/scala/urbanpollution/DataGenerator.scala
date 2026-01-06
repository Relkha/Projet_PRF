package urbanpollution

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.util.Random
import java.io.{File, PrintWriter}

/**
 * Générateur de dataset simulé (CSV) — conforme au sujet :
 * - stations.csv
 * - edges.csv
 * - measurements.csv
 */
object DataGenerator {

  private val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val outDir = if (args.length >= 1) args(0) else "data"
    val nStations = if (args.length >= 2) args(1).toInt else 20
    val nDays = if (args.length >= 3) args(2).toInt else 14

    new File(outDir).mkdirs()
    new File(s"$outDir/stream_samples").mkdirs()
    new File(s"$outDir/stream").mkdirs()

    val rnd = new Random(42)

    val stationIds = (1 to nStations).map(i => f"S$i%02d").toList
    val lineIds = List("L1","L2","L3","L4","L5")

    // -------- stations.csv --------
    val stWriter = new PrintWriter(new File(s"$outDir/stations.csv"))
    stWriter.println("station_id,name,lat,lon,zone,transport_type")

    stationIds.foreach { sid =>
      val lat = 48.80 + rnd.nextDouble()*0.10
      val lon = 2.25 + rnd.nextDouble()*0.20
      val zone = if (rnd.nextDouble() < 0.55) "centre" else "peripherie"
      val ttype = rnd.shuffle(List("metro","tram","bus")).head
      stWriter.println(s"$sid,Station_$sid,$lat,$lon,$zone,$ttype")
    }
    stWriter.close()

    // -------- edges.csv (connexions par "lignes") --------
    val edWriter = new PrintWriter(new File(s"$outDir/edges.csv"))
    edWriter.println("src_station_id,dst_station_id,distance_m,line_id")

    val perLine = Math.max(4, nStations / 3)
    val grouped = stationIds.grouped(perLine).toList

    grouped.zipWithIndex.foreach { case (grp, li) =>
      val lid = lineIds(li % lineIds.length)
      grp.sliding(2).foreach {
        case List(a,b) =>
          val dist = 400 + rnd.nextInt(1500)
          edWriter.println(s"$a,$b,$dist,$lid")
          edWriter.println(s"$b,$a,$dist,$lid")
        case _ =>
      }
    }

    // quelques transferts aléatoires
    (1 to Math.max(3, nStations/5)).foreach { _ =>
      val a = stationIds(rnd.nextInt(nStations))
      val b = stationIds(rnd.nextInt(nStations))
      if (a != b) {
        val dist = 250 + rnd.nextInt(1200)
        edWriter.println(s"$a,$b,$dist,TRANSFER")
        edWriter.println(s"$b,$a,$dist,TRANSFER")
      }
    }
    edWriter.close()

    // -------- measurements.csv --------
    val mWriter = new PrintWriter(new File(s"$outDir/measurements.csv"))
    mWriter.println("timestamp,station_id,line_id,co2,pm25,noise_db,humidity,temp_c,passenger_traffic,is_holiday,event_level")

    val start = LocalDateTime.of(2025, 10, 1, 0, 0)

    def isPeak(h: Int): Boolean = (h >= 7 && h <= 9) || (h >= 17 && h <= 19)

    (0 until nDays).foreach { d =>
      val date = start.plusDays(d.toLong)
      val holiday = if (d % 7 == 6) 1 else 0 // dimanche simulé
      val event = if (d % 10 == 0) 2 else 0

      (0 until 24).foreach { h =>
        val ts = date.plusHours(h.toLong)
        stationIds.foreach { sid =>
          val line = lineIds(rnd.nextInt(lineIds.length))
          val peak = isPeak(h)

          // trafic : plus élevé en pointe + bruit + CO2 + PM2.5 corrélés
          val baseTraffic = if (holiday == 1) 80 else 180
          val traffic = baseTraffic + (if (peak) 220 else 0) + rnd.nextInt(120)
          val temp = 12.0 + 8.0*Math.sin((h/24.0)*2*Math.PI) + rnd.nextGaussian()*0.7
          val hum = 45.0 + rnd.nextGaussian()*10.0
          val noise = 35.0 + 0.03*traffic + rnd.nextGaussian()*3.0 + (if (event > 0) 3 else 0)

          val co2 = 420.0 + 0.12*traffic + rnd.nextGaussian()*15.0
          val pm25 = 10.0 + 0.018*traffic + 0.05*Math.max(0, hum-50) + rnd.nextGaussian()*4.0 + (if (event > 0) 5 else 0)

          // injections de quelques anomalies
          val isAnom = rnd.nextDouble() < 0.003
          val pm25Final = if (isAnom) pm25 + 80 + rnd.nextInt(50) else pm25

          mWriter.println(
            s"${ts.format(fmt)},$sid,$line," +
            f"$co2%.2f," +
            f"$pm25Final%.2f," +
            f"$noise%.2f," +
            f"$hum%.2f," +
            f"$temp%.2f," +
            s"$traffic,$holiday,$event"
          )
        }
      }
    }
    mWriter.close()

    // -------- stream samples (mini-fichiers CSV) --------
    // On prend les dernières 6h de la dernière journée et on les découpe
    val sampleWriterBase = (0 until 6).map(i => new PrintWriter(new File(s"$outDir/stream_samples/batch_$i.csv"))).toArray
    sampleWriterBase.foreach(_.println("timestamp,station_id,line_id,co2,pm25,noise_db,humidity,temp_c,passenger_traffic,is_holiday,event_level"))

    val lastDay = start.plusDays((nDays-1).toLong)
    (0 until 6).foreach { i =>
      val ts = lastDay.plusHours((18 + i).toLong)
      stationIds.foreach { sid =>
        val line = lineIds(rnd.nextInt(lineIds.length))
        val traffic = 200 + rnd.nextInt(250)
        val temp = 10.0 + rnd.nextGaussian()*1.0
        val hum = 55.0 + rnd.nextGaussian()*8.0
        val noise = 40.0 + 0.03*traffic + rnd.nextGaussian()*3.0
        val co2 = 430.0 + 0.12*traffic + rnd.nextGaussian()*15.0
        val pm25 = 12.0 + 0.02*traffic + rnd.nextGaussian()*4.0
        sampleWriterBase(i).println(s"${ts.format(fmt)},$sid,$line,${co2}%,.2f,${pm25}%,.2f,${noise}%,.2f,${hum}%,.2f,${temp}%,.2f,$traffic,0,0")
      }
    }
    sampleWriterBase.foreach(_.close())

    println(s"[OK] Dataset généré dans $outDir")
  }
}
