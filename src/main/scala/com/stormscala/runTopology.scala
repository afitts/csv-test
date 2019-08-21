package com.stormscala

import org.apache.storm.{Config, LocalCluster}
import org.apache.storm.generated.Bolt
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.tuple.Fields
import org.apache.storm.utils.Utils.findAndReadConfigFile

object runTopology {
  def main(args: Array[String]): Unit = {
    val builder = new TopologyBuilder
    builder.setSpout("CsvSpout", new CsvSpout("/Users/afitts/projects/intro-to-storm/sample.csv",
      separator = ',', false))
    builder.setBolt("PrintingBolt", new PrintingBolt).shuffleGrouping("CsvSpout", "csv-files")//fieldsGrouping("CsvSpout",new Fields("type","sensor","filename"))
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
