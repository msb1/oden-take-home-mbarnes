import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

object Main {

  val logger: Logger = LoggerFactory.getLogger("Main")

  case class WebLog(ipAddress: String, timestamp: String, numBytes: Long)

  case class LogResults(ipAddress: String, totalBytes: Long)

  case class WindowedLogResults(ipAddress: String, windowTime: Long, totalBytes: Long)

  val pattern = "dd:HH:mm:ss"
  val formatter = new SimpleDateFormat(pattern)
  val localDateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)
  val epaHttpFilePath = "/home/bw/epa-http.txt"

  def main(args: Array[String]) {

    // create or get SparkSessions
    val spark: SparkSession = SparkSession
      .builder
      .master("local[4]")
      .appName("OdenSpark")
      .config("forceDeleteTempCheckpointLocation", value = true)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
    import spark.implicits._

    // read in text file as String DataFrame and transform through case class to DataFrame with schema
    val epaStringLogDF = spark.read.textFile(epaHttpFilePath)
    println(s"\n\nNumber of rows: ${epaStringLogDF.count()}")
    val epaWebLogDF = epaStringLogDF.map(x => {
      val chunks = x.trim.split(" ")
      val last = chunks.size - 1
      val numBytes = if (chunks(last).trim.equals("-")) 0 else chunks(last).toLong
      val timestamp = StringUtils.substringBetween(x, "[", "]")
      WebLog(chunks(0), timestamp, numBytes)
    }).toDF()

    epaWebLogDF.printSchema()
    epaWebLogDF.show(false)

    // process entire file dataset - groupBy IP Address and sum numBytes
    val resultsDF = epaWebLogDF
      .groupBy("ipAddress")
      .sum("numBytes")
      .collect()
      .map(v => {
        val bw = new BufferedWriter(new FileWriter(new File("epa-results.txt"), true))
        val line = s"IP Address: ${v(0)} has received ${v(1)} bytes"
        bw.write(s"$line\n")
        println(line)
        bw.close()
      })
    //.show(false)

    // window and groupby IP Address and window - sum numBytes for each IP Address per window
    val windowedResultsDF = epaWebLogDF
      .select(
        col("ipAddress"),
        to_timestamp(col("timestamp"), "dd:HH:mm:ss").as("datetime"),
        col("numBytes")
      )
      .withWatermark("datetime", "2 minutes")
      .groupBy(col("ipAddress"), window(col("datetime"), "60 minutes"))
      .agg(sum(col("numBytes")).as("totalBytes"))
      .select(
        col("ipAddress"),
        date_format($"window.start", "dd:HH:mm:ss").alias("windowtime"),
        col("totalBytes")
      )
      .orderBy("windowtime")
      .collect()
      .map(v => {
        val bw = new BufferedWriter(new FileWriter(new File("windowed-epa-results.txt"), true))
        val line = s"Window Start: ${v(1)} for IP Address: ${v(0)} has received ${v(2)} bytes"
        bw.write(s"$line\n")
        println(line)
        bw.close()
      })
      //.show(false)

    spark.stop()
  }
}
