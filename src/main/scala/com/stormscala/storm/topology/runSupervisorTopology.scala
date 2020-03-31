package com.stormscala.storm.topology

import java.io.{BufferedReader, FileReader}
import java.util.Properties

import com.stormscala.storm.bolt.CoordinatorBolt
import com.stormscala.storm.spout.CsvToKafkaSpout
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.storm.tuple.{Fields, Values}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.storm.kafka.bolt.KafkaBolt
import org.apache.storm.kafka.spout.{Func, KafkaSpout, KafkaSpoutConfig}
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.{Config, LocalCluster}

object runSupervisorTopology {
  def main(args: Array[String]): Unit = {
    val pathname = "/Users/afitts/IdeaProjects/storm-metrics/src/main"
    val builder = new TopologyBuilder
    builder.setSpout("CsvtoKafkaSpout", new CsvToKafkaSpout(s"$pathname/sample.csv",
      separator = ',', true))
    val fileName = s"$pathname/supervisor-test.json"
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
      //val response = (new DefaultHttpClient).execute(post) Old deprecated version.
      val client = HttpClientBuilder.create.build
      val response = client.execute(post)
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

    /* Build a Kafka spout for ingesting enriched truck data
     */

    /* Scala-integration-fix snippet:
     *
     * Construct a record translator that defines how to extract and turn
     * a Kafka ConsumerRecord into a list of objects to be emitted
     */
    //lazy val GetRequestTranslator = new Func[ConsumerRecord[String, String], java.util.List[AnyRef]] {
    //  def apply(record: ConsumerRecord[String, String]) = new Values("GetRequestData", record.value())
    //}

    val GetRequestSpoutConfig: KafkaSpoutConfig[String, String] = KafkaSpoutConfig.builder("localhost:9092", "policy-to-coord")
      //.setRecordTranslator(GetRequestTranslator, new Fields("key", "data"))
      .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST)
      .setGroupId("g") //Setting a group ID is mandatory for some reason. will get an error if you don't set one
      .build()

    // Create a spout with the specified configuration, with only 1 instance of this bolt running in parallel, and place it in the topology blueprint
    builder.setSpout("get-CsvtoKafkaSpout", new CsvToKafkaSpout(s"$pathname/sample-get-request.csv", separator = ',', true))
    val bolt1 = new KafkaBolt()
      .withProducerProperties(kafkaProps)
      .withTopicSelector(new DefaultTopicSelector("policy-to-coord"))
      .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "data"))
    builder.setBolt("get-forwardToKafka", bolt1, 8).shuffleGrouping("get-CsvtoKafkaSpout", "csv-files")
    builder.setSpout("KafkaToCoordinator", new KafkaSpout(GetRequestSpoutConfig), 1)
    builder.setBolt("CoordinatorFromKafka", new CoordinatorBolt,1).shuffleGrouping("KafkaToCoordinator")

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
