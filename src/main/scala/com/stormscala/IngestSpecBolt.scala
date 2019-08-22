package com.stormscala

import java.io._
import java.util

import com.codahale.metrics.Counter
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple
import org.slf4j.{Logger, LoggerFactory}

// This bolt is for submitting a new datasource to druid (as an example i've done a dns.log).


class IngestSpecBolt extends BaseRichBolt {
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
    val fileName = "/Users/afitts/IdeaProjects/csv-test/src/main/ingest-test.json"
    val reader = new BufferedReader(new FileReader(fileName))
    val IngestSpecJson = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")
    try {
      logger.warn(s"BOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO, $f1,$f2,$f3") //println(tuple
      this.tupleCounter.inc()
      // create an HttpPost object
      val post = new HttpPost("http://localhost:8081/druid/indexer/v1/task")

      // set the Content-type
      post.setHeader("Content-type", "application/json")

      // add the JSON as a StringEntity
      post.setEntity(new StringEntity(IngestSpecJson))
      logger.warn(s"WOAHHHHHHHHHHHHHHHH, $post")
      // send the post request
      val response = (new DefaultHttpClient).execute(post)
      logger.warn(s"CHECK MEMEMEMEMEMEMEMEMEMEME, $response")
      // print the response headers
      println("--- HEADERS ---")
      response.getAllHeaders.foreach(arg => println(arg))
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
  }
}