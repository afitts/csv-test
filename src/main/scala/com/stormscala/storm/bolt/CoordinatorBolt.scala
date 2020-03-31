package com.stormscala.storm.bolt

import java.io.BufferedWriter
import java.util

import com.codahale.metrics.Counter
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import org.apache.storm.topology.base.{BaseBasicBolt, BaseRichBolt}
import org.apache.storm.tuple.{Fields, Tuple, Values}
import org.slf4j.{Logger, LoggerFactory}
import java.util.concurrent.{CancellationException, ExecutionException, TimeUnit}
import java.util.{Collections, Properties, UUID}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import com.stormscala.common.utilities.JsonConverter
import org.apache.http.util.EntityUtils
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, CreateTopicsOptions, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class CoordinatorBolt extends BaseRichBolt{
  val logger: Logger = LoggerFactory.getLogger(classOf[CoordinatorBolt])
  var collector: OutputCollector = _
  var tupleCounter: Counter = _
  var output: BufferedWriter = _
  val kafkaBrokers: String = "localhost:9092"

  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats = org.json4s.DefaultFormats

    parse(jsonStr).extract[Map[String, Any]]
  }
  object JsonUtil {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    def toJson(value: Map[Symbol, Any]): String = {
      toJson(value map { case (k,v) => k.name -> v})
    }

    def toJson(value: Any): String = {
      mapper.writeValueAsString(value)
    }

    def toMap[V](json:String)(implicit m: Manifest[V]) = fromJson[Map[String,V]](json)

    def fromJson[T](json: String)(implicit m : Manifest[T]): T = {
      mapper.readValue[T](json)
    }
  }
  def createKafkaTopic(topic: String, partitions: Int = 30, replicationFactor: Short = 3): Unit = {
    // Kafka properties
    val kafkaAdminProps: Properties = new Properties()
    kafkaAdminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaBrokers)
    kafkaAdminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,10000:java.lang.Integer)

    val adminClient: AdminClient = AdminClient.create(kafkaAdminProps)
    val topicList: util.Set[String] = adminClient.listTopics().names().get(10,TimeUnit.SECONDS)

    // This is a conscious decision to either create the topic or silently do nothing if the topic already exists
    if(!topicList.contains(topic)) {
      try {
        val future: KafkaFuture[Void] = adminClient.createTopics(Collections.singleton(new NewTopic(topic,partitions,replicationFactor)),new CreateTopicsOptions().timeoutMs(10000)).all()
        future.get(10,TimeUnit.SECONDS)
      } catch {
        case e @ (_: InterruptedException | _: ExecutionException) =>
          logger.error(s"ClassOf[KafkaWriter]:\ncreateKafkaTopic() Attempt to create Kafka topic failed.\nERROR MESSAGE: ${e.getMessage}\n",e)
      }
    }
  }

  override def prepare(conf: util.Map[_, _], context: TopologyContext, stormCollector: OutputCollector): Unit = {
    collector = stormCollector
    this.tupleCounter = context.registerCounter("tupleCount")

  }

  override def execute(input: Tuple): Unit = {
    //val httpmethod: Any = input.getValue(0)//getStringByField("httpmethod") This is the topic name
    //val command: Any = input.getValue(1)//getStringByField("command") Partition
    //val ttt: Any = input.getValue(2) //Offset
    //val bbb: Any = input.getValue(3) //key
    //val ccc: Any = input.getString(4) //value
    //logger.warn(s"WOWOWOWOWOWOWOWOWOWOWOWOWOWOWOW $httpmethod | $command | $ttt | $bbb | $ccc")
    val json: String = input.getString(4)
    val jsonMap = jsonStrToMap(json)
    val map = JsonUtil.toMap[Object](json)
    logger.warn(s"WOWOWOWOWOWOWOWOWOWOWOWOWOWOWOW $json | $jsonMap | $map") // Have two different methods for converting json str to map
    val httpmethod = map("httpmethod")
    val command = map("command") //not sure why an extra space is being placed after command
    //val dataSourceName: String = tuple.getStringByField("dataSourceName")
    val kafkaTopicName: String = s"coord-to-policy"

    // Make sure we have created the Kafka topic
    createKafkaTopic(kafkaTopicName)

    //if (action == "load") {
    //  val tieredReplicants: AnyRef = tuple.getValueByField("tieredReplicants")
    //  if (ruleType == "loadByInterval") {
    //    val interval: String = tuple.getStringByField("interval")
    //  }
    //}
    //else if (action == "drop") {

    //}
    //else if (action == "broadcast") {

    //}
    try {
      if (httpmethod == "post") {
        val CoordinatorJson: String = input.getStringByField("CoordinatorJson") // Send this Json commnand to coordinator
        val postRequest = new HttpPost(s"http://localhost:8081/druid/coordinator/v1/$command") /// druid/coordinator/v1/rules/{dataSourceName}
        // add the JSON as a StringEntity
        postRequest.setEntity(new StringEntity(CoordinatorJson))
        // send the post request
        //val response = (new DefaultHttpClient).execute(post) Old deprecated version.
        val client = HttpClientBuilder.create.build
        val response = client.execute(postRequest)
        // print the response headers
        println("--- HEADERS ---")
        response.getAllHeaders.foreach(arg => println(arg))

        //Possibly create JSON rule for posting.
        //val tfields: mutable.LinkedHashMap[String,Any] = mutable.LinkedHashMap[String,Any](
        //  "ts"           -> l(0),
        //  "type"          -> l(1),
        //  "sensor"        -> l(2),
        //  "filename"      -> l(3)
        //)
        //val csvAsJson = JsonConverter.toJson(tfields.toMap)
        //collector.emit("csv-files",new Values("0",csvAsJson))
      } else if (httpmethod == "get") {
        val getRequest = new HttpGet(s"http://localhost:8081/druid/coordinator/v1/$command")
        val client = HttpClientBuilder.create.build
        val response = client.execute(getRequest)
        //Now pull back the response object
        val httpEntity = response.getEntity
        val apiOutput = EntityUtils.toString(httpEntity)
        //println(s"LALALALALALALALA $apiOutput")
        // print the response headers
        //println("--- HEADERS ---")
        //response.getAllHeaders.foreach(arg => println(arg))


        //val csvAsJson = JsonConverter.toJson(tfields)

        //collector.emit(new Values(httpmethod, apiOutput))

        // Now let's send the response for the get request to the appropriate topic.
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

        val producer: KafkaProducer[String,String] = new KafkaProducer[String,String](kafkaProps)

        val producerRecord: ProducerRecord[String,String] = new ProducerRecord[String,String](kafkaTopicName,apiOutput)

        try {
          producer.send(producerRecord).get()
        } catch {
          case e @ (_: CancellationException | _: ExecutionException | _: InterruptedException) =>
            logger.error(s"ClassOf[KafkaWriter]:\nAttempt to send kafka message failed.\nERROR MESSAGE: ${e.getMessage}\n",e)
        }
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
  }

}
