package com.stormscala.storm.bolt

import java.util

import com.codahale.metrics.Counter
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple
import org.slf4j.{Logger, LoggerFactory}

class TupleCountingBolt extends BaseRichBolt {
  val logger: Logger = LoggerFactory.getLogger(classOf[TupleCountingBolt])
  var collector: OutputCollector = _
  var tupleCounter: Counter = _

  override def prepare(conf: util.Map[_, _], context: TopologyContext, stormCollector: OutputCollector): Unit = {
    this.tupleCounter = context.registerCounter("tupleCount")

  }

  override def execute(input: Tuple): Unit = {
    this.tupleCounter.inc()
  }
  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
  }
}
