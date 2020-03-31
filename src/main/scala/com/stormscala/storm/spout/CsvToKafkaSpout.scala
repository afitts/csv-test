package com.stormscala.storm.spout

import java.io._
import java.util
import java.util.concurrent.atomic.AtomicLong
import com.stormscala.common.utilities.JsonConverter
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.{Fields, Values}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable


/**
 * This spout reads data from a CSV file. It is only suitable for testing in local mode
 */
class CsvToKafkaSpout(val fileName: String, val separator: Char, var includesHeaderRow: Boolean) extends BaseRichSpout {
  var _collector: SpoutOutputCollector = _
  var reader: BufferedReader = _//CSVReader = _
  var linesRead: AtomicLong = _
  linesRead = new AtomicLong(0)
  val logger: Logger = LoggerFactory.getLogger(classOf[CsvSpout])
  var line: String = _
  var cols: Array[String] = _

  override def open(conf: util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit = {
    this._collector = collector
    try {
      reader = new BufferedReader(new FileReader(fileName))
      //reader = CSVReader.open(new File(fileName))
      // read and ignore the header if one exists
      if (includesHeaderRow) {
        line = reader.readLine()
        cols = line.split(",")
      }//readNext()
    } catch {
      case e: Exception =>
        throw new RuntimeException(e)
    }
  }

  override def nextTuple(): Unit = {
    try {
      line = reader.readLine()//Next().toList.flatten.mkString(",")
      if (line != null) {
        val id = linesRead.incrementAndGet
        //val a = new Values(line.split(","))
        val l: Array[String] = line.split(",")
        val tfields = (cols zip l).toMap
        logger.warn(s"EXXXXXXAAAAAAAAAAAAAAAAAAAAMPLE $tfields")
        //val tfields: mutable.LinkedHashMap[String,Any] = mutable.LinkedHashMap[String,Any](
        //  "ts"           -> l(0),
        //  "type"          -> l(1),
        //  "sensor"        -> l(2),
        //  "filename"      -> l(3)
        //)
        //val csvAsJson = JsonConverter.toJson(tfields.toMap)
        val csvAsJson = JsonConverter.toJson(tfields)
        _collector.emit("csv-files",new Values("0",csvAsJson))
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
    declarer.declareStream("csv-files", new Fields("key", "data"))
  }
}

