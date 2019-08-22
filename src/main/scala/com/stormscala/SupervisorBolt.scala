package com.stormscala

import java.io.{BufferedWriter, FileWriter}
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

//This bolt submits a new supervisor to druid. This supervisor listens to a particular Kafka topic for incoming data and
//creates a datasource to house the data. This is the streaming data version of the NewIngestSpecBolt.

class SupervisorBolt extends BaseRichBolt {
  val logger: Logger = LoggerFactory.getLogger(classOf[SupervisorBolt])
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
    val SupervisorJson = """{
      "type": "kafka",
      "dataSchema": {
        "dataSource": "dns",
        "parser": {
          "type": "string",
          "parseSpec": {
          "format": "json",
          "timestampSpec": {
            "column": "ts",
            "format": "posix"
      },
        "dimensionsSpec": {
          "dimensions": [
          "AA",
          "RA",
          "RD",
          "TC",
          "TTLs",
          "answers",
          "dst",
          "proto",
          "qclass_name",
          "qtype_name",
          "query",
          "rcode_name",
          "rejected",
          "src",
          "uid"
          ]
        }
        }
        },
          "metricsSpec": [
        {
          "name": "count",
          "type": "count"
        },
        {
          "name": "sum_Z",
          "type": "longSum",
          "fieldName": "Z"
        },
        {
          "name": "sum_dport",
          "type": "longSum",
          "fieldName": "dport"
        },
        {
          "name": "sum_qclass",
          "type": "longSum",
          "fieldName": "qclass"
        },
        {
          "name": "sum_qtype",
          "type": "longSum",
          "fieldName": "qtype"
        },
        {
          "name": "sum_rcode",
          "type": "longSum",
          "fieldName": "rcode"
        },
        {
          "name": "sum_sport",
          "type": "longSum",
          "fieldName": "sport"
        },
        {
          "name": "sum_trans_id",
          "type": "longSum",
          "fieldName": "trans_id"
        }
          ]
        },
      "tuningConfig": {
        "type": "kafka",
        "reportParseExceptions": false
      },
      "ioConfig": {
        "topic": "dns",
        "replicas": 1,
        "taskDuration": "PT10M",
        "completionTimeout": "PT20M",
        "consumerProperties": {
        "bootstrap.servers": "localhost:9092"
      }
      }
    }"""
    try {
      logger.warn(s"HELLLLOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO, $f1,$f2,$f3") //println(tuple
      this.tupleCounter.inc()
      // create an HttpPost object
      val post = new HttpPost("http://localhost:8081/druid/indexer/v1/supervisor")

      // set the Content-type
      post.setHeader("Content-type", "application/json")

      // add the JSON as a StringEntity
      post.setEntity(new StringEntity(SupervisorJson))
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
