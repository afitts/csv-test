package com.stormscala.storm.bolt

import java.io.{BufferedWriter, FileWriter}
import java.util
import java.io.File

import com.codahale.metrics.Counter
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.{BaseBasicBolt, BaseRichBolt}
import org.apache.storm.tuple.Tuple
import org.slf4j.{Logger, LoggerFactory}

class PrintingBolt extends BaseRichBolt {
  val logger: Logger = LoggerFactory.getLogger(classOf[PrintingBolt])
  var collector: OutputCollector = _
  var tupleCounter: Counter = _
  var output: BufferedWriter = _

  override def prepare(conf: util.Map[_, _], context: TopologyContext, stormCollector: OutputCollector): Unit = {
    collector = stormCollector
    this.tupleCounter = context.registerCounter("tupleCount")

  }

  override def execute(tuple: Tuple): Unit = {
    val f1: String = tuple.getStringByField("type")
    val f2: String = tuple.getString(1)
    val f3: String = tuple.getStringByField("filename")
    try {
      logger.warn(s"HELLLLOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO, $f1,$f2,$f3") //println(tuple
      this.tupleCounter.inc()
      output = new BufferedWriter(new FileWriter("/Users/afitts/projects/intro-to-storm/hh.txt", true))
      output.append(s"${tuple.getString(0)}, ${tuple.getString(1)}, ${tuple.getString(2)}\n")
      output.close()
      collector.ack(tuple)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
  }

  override def cleanup(): Unit = {
    println("GOOOODBYYYYYYYYYYYYYYYYYYYYYYYYYYE")
  }
}
