import java.io.{BufferedWriter, File, FileWriter}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Duration
import java.time.format.DateTimeFormatter

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import ch.qos.logback.classic.{Level, Logger}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{AnyWithOperations, EnvironmentSettings, FieldExpression, LiteralIntExpression, Table, Tumble}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor

object Main extends App {

  implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "oden-flink")
  implicit val executionContext: ExecutionContextExecutor = system.executionContext

  LoggerFactory.getLogger("org").asInstanceOf[Logger].setLevel(Level.WARN)
  LoggerFactory.getLogger("akka").asInstanceOf[Logger].setLevel(Level.WARN)

  println("Oden Flink Take Home Exercise program start...")

  case class WebLog(ipAddress: String, timestamp: Long, numBytes: Long)

  case class LogResults(ipAddress: String, totalBytes: Long)

  case class WindowedLogResults(ipAddress: String, windowTime: Timestamp, totalBytes: Long)

  val filePath = "/home/bw/epa-http.txt"

  val pattern = "dd:HH:mm:ss"
  val formatter = new SimpleDateFormat(pattern)
  val localDateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(pattern)

  // initialize stream and table environments
  val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val tEnv = StreamTableEnvironment.create(env, settings)
  env.setParallelism(1)

  // read text file as datastream and map string to WebLog case class
  val webStrings: DataStream[String] = env.readTextFile(filePath)
  val webLogs: DataStream[WebLog] = webStrings
    .map(x => {
      val chunks = x.trim.split(" ")
      val last = chunks.size - 1
      val numBytes = if (chunks(last).trim.equals("-")) 0 else chunks(last).toLong
      val timestamp = formatter.parse(StringUtils.substringBetween(x, "[", "]")).getTime
      WebLog(chunks(0), timestamp, numBytes)
    })
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness[WebLog](Duration.ofSeconds(120))
        .withTimestampAssigner(new SerializableTimestampAssigner[WebLog] {
          override def extractTimestamp(webLog: WebLog, recordTimestamp: Long): Long = webLog.timestamp
        }))

  //
  val webLogTable: Table = tEnv.fromDataStream(webLogs, $"ipAddress", $"timestamp".rowtime, $"numBytes")
  val results = webLogTable
    .window(Tumble over 25.hours on $"timestamp" as "w")
    .groupBy($"ipAddress", $"w") // group by key and window
    .select($"ipAddress", $"numBytes".sum as "totalBytes")

  val windowedResults = webLogTable
    .window(Tumble over 1.hour on $"timestamp" as "w")
    .groupBy($"ipAddress", $"w") // group by key and window
    .select($"ipAddress", $"w".start, $"numBytes".sum as "totalBytes")

  // print & write results for entire dataset byte counts
  val stream = tEnv.toRetractStream[LogResults](results)
    .map(v => {
      val bw = new BufferedWriter(new FileWriter(new File("epa-results.txt"), true))
      val line = s"IP Address: ${v._2.ipAddress} has received ${v._2.totalBytes} bytes"
      bw.write(s"$line\n")
      println(line)
      bw.close()
    })

  // print & write results for windowed byte counts
  val windowedStream = tEnv.toRetractStream[WindowedLogResults](windowedResults)
    .map(v => {
      val bw = new BufferedWriter(new FileWriter(new File("windowed-epa-results.txt"), true))
      val line = s"Window Start: ${v._2.windowTime.toLocalDateTime.minusHours(5).format(localDateTimeFormatter)} for IP Address: ${v._2.ipAddress} has received ${v._2.totalBytes} bytes"
      bw.write(s"$line\n")
      println(line)
      bw.close()
    })

  // execute program
  env.execute("Oden-Flink")

}
