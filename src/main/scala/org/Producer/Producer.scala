package org.Producer

import java.util.Properties

import org.Utils.ConfigManager
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

/**
  * 模拟发送数据
  * 利用kafka生产者每隔0.01s发送1条数据
  */
object Producer extends  App {
  //创建生产者需要的properties
  val prop = new Properties()
  //kafka所在的服务器
  prop.put("bootstrap.servers",ConfigManager.getProp("bootstrap.servers"))
  //producer发送的消息keyvalue的序列化方式
  prop.put("key.serializer",ConfigManager.getProp("key.serializer"))
  prop.put("value.serializer",ConfigManager.getProp("value.serializer"))
  //创建生产者
  val producer = new KafkaProducer[String,String](prop)
  //导入需要发送的数据文件
  val file = Source.fromFile(ConfigManager.getProp("filePath"))
  try{
    //每隔10ms发送一行
    for(line <- file.getLines()){
      producer.send(new ProducerRecord[String,String](ConfigManager.getProp("topicName"),line))
      Thread.sleep(10)
    }
  }catch {
    //如果报错，输出错误信息
    case e:Exception=>{
      e.printStackTrace()
    }
  }
  //输出完毕关闭producer
  producer.close()
  print("发送完毕")
}
