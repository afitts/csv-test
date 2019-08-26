package com.stormscala.storm.topology

import java.io.{BufferedReader, FileReader}
import java.util.Properties

import com.stormscala.storm.spout.CsvToKafkaSpout
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.storm.kafka.bolt.KafkaBolt
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.{Config, LocalCluster}

object runSupervisorTopology {
  def main(args: Array[String]): Unit = {
    val builder = new TopologyBuilder
    builder.setSpout("CsvtoKafkaSpout", new CsvToKafkaSpout("/Users/afitts/projects/intro-to-storm/sample.csv",
      separator = ',', false))
    val fileName = "/Users/afitts/IdeaProjects/csv-test/src/main/supervisor-test.json"
    val reader = new BufferedReader(new FileReader(fileName))
    val SupervisorJson = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")
    try {
      // create an HttpPost object
      val post = new HttpPost("http://localhost:8081/druid/indexer/v1/supervisor")

      // set the Content-type
      post.setHeader("Content-type", "application/json")

      // add the JSON as a StringEntity
      post.setEntity(new StringEntity(SupervisorJson))
      // send the post request
      val response = (new DefaultHttpClient).execute(post)
      // print the response headers
      println("--- HEADERS ---")
      response.getAllHeaders.foreach(arg => println(arg))
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }

    // Kafka properties
    val kafkaProps: Properties = new Properties()
    val kafkaBrokers: String = "localhost:9092"
    kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaBrokers)
    kafkaProps.put(ProducerConfig.ACKS_CONFIG,"1")
    kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG,"32768")
    kafkaProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"268435456")
    kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy")
    kafkaProps.put(ProducerConfig.RETRIES_CONFIG,10:java.lang.Integer)
    kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG,s"storm-producer")//bro-storm-producer-${UUID.randomUUID().toString}")
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

    val bolt = new KafkaBolt()
      .withProducerProperties(kafkaProps)
      .withTopicSelector(new DefaultTopicSelector("sample-storm"))
      .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "data"))
    builder.setBolt("forwardToKafka", bolt, 8).shuffleGrouping("CsvtoKafkaSpout", "csv-files")


    val conf = new Config()
    conf.setDebug(false)
    conf.setMaxTaskParallelism(3)
    //conf.registerMetricsConsumer(org.apache.storm.metrics2.reporters.)
    //conf.put(Config.)
    val cluster = new LocalCluster
    cluster.submitTopology("csv-test", conf, builder.createTopology())
    Thread.sleep(10000)
    println("Goodbye, world!")
    cluster.shutdown()
    println("I am Dead!")
  }
}
