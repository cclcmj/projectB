package org.handle

import org.Utils.{ConfigManager, StreamDirect}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

object BussinessApp extends App {
  val conf = new SparkConf().setAppName("bussiness").setMaster("local[*]")
    .set("spark.streaming.kafka.maxRatePerPartition",ConfigManager.getProp("maxRatePerPartition"))
    // 设置序列化机制
    .set("spark.serlizer",ConfigManager.getProp("spark.serlizer"))
  val ssc = new StreamingContext(conf,Seconds(10))
  //广播变量
  val cityline = Source.fromFile(ConfigManager.getProp("cityPath")).getLines()
  var citymap =  Map[String,String]()
  for(line<-cityline) {
    citymap += (line.split("\\s")(0)->line.split("\\s")(1))
  }
  val broad = ssc.sparkContext.broadcast(citymap)
  val stream = StreamDirect.getStream(ssc)
  stream.foreachRDD(rdd=>StreamDirect.handleAndSaveOffset(rdd
    ,(BussinessHandle.middleArgsRDD _).curried(broad.value)
    ,BussinessHandle.result01_01
    ,BussinessHandle.result01_02
    ,BussinessHandle.result02
    ,BussinessHandle.result03))
  ssc.start()
  ssc.awaitTermination()
}