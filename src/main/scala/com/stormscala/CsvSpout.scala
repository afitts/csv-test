package com.stormscala

import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.{Fields, Values}
import org.apache.storm.metrics2.reporters.ConsoleStormReporter
import java.io._
import java.util
import java.util.concurrent.atomic.AtomicLong

import com.github.tototoshi.csv._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._


/**
 * This spout reads data from a CSV file. It is only suitable for testing in local mode
 */
class CsvSpout(val fileName: String, val separator: Char, var includesHeaderRow: Boolean) extends BaseRichSpout {
  var _collector: SpoutOutputCollector = _
  var reader: BufferedReader = _//CSVReader = _
  var linesRead: AtomicLong = _
  linesRead = new AtomicLong(0)
  val logger: Logger = LoggerFactory.getLogger(classOf[CsvSpout])
  var line: String = _

  override def open(conf: util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit = {
    this._collector = collector
    try {
      reader = new BufferedReader(new FileReader(fileName))
      //reader = CSVReader.open(new File(fileName))
      // read and ignore the header if one exists
      if (includesHeaderRow) reader.readLine()//readNext()
    } catch {
      case e: Exception =>
        throw new RuntimeException(e)
    }
  }

  override def nextTuple(): Unit = {
    try {
      val line: String = reader.readLine()//Next().toList.flatten.mkString(",")
      if (line != null) {
        val id = linesRead.incrementAndGet
        //val a = new Values(line.split(","))
        logger.warn(s"EXXXXXXAAAAAAAAAAAAAAAAAAAAMPLE $line")
        val l: Array[String] = line.split(",")
        _collector.emit("csv-files",new Values(l(0),l(1),l(2)))
      }
      else Thread.sleep(1)//.out.println("Finished reading file, " + linesRead.get + " lines read")
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  override def ack(id: Any): Unit = {
  }

  override def fail(id: Any): Unit = {
    System.err.println("Failed tuple with id " + id)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declareStream("csv-files", new Fields("type","sensor","filename"))
  }
}
