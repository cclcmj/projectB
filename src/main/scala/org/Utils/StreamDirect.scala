package org.Utils

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import java.{lang, util}

import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD

/**
  * SparkStream以Direct方式连接Kafka
  * getOffset:获取offset
  * handleAndSaveOffset:更新offset。并且执行输入的可变数量的函数参数
  * getStream:获取流
  */
object StreamDirect {
  val kafkas = Map[String,Object](
    "bootstrap.servers"->ConfigManager.getProp("bootstrap.servers"),
    "key.deserializer"->classOf[StringDeserializer],
    "value.deserializer"->classOf[StringDeserializer],
    "group.id"->ConfigManager.getProp("groupId"),
    "auto.offset.reset"->ConfigManager.getProp("auto.offset.reset"),
    "enable.auto.commit" -> (false:lang.Boolean)
  )
  val topics = Set(ConfigManager.getProp("topicName"))
  //获取Stream
  def getStream(ssc:StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val fromOffset = getOffset
    if(fromOffset.size == 0){
      KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topics,kafkas))
    }else{
      KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Assign[String,String](fromOffset.keys,kafkas,fromOffset))
    }
  }
  //获取offset
  def getOffset: Map[TopicPartition, Long] = {
    //获取jedis连接
    val jedis = JedisConnectionPool.getConnection()
    //获取所有的Topic、Partition
    val topicPartitionOffset: util.Map[String, String]=jedis.hgetAll(ConfigManager.getProp("groupId"))
    //将map转为list循环处理
    import scala.collection.JavaConversions._
    val topicPartitionOffsetList:List[(String,String)]=topicPartitionOffset.toList
    //定义循环状态变量，并循环处理
    var  fromdbOffset = Map[TopicPartition,Long]()
    for(topicPL<-topicPartitionOffsetList) {
      val str = topicPL._1.split("[-]")
      fromdbOffset += (new TopicPartition(str(0),str(1).toInt)->topicPL._2.toLong)
    }
    jedis.close()
    fromdbOffset
  }
  //处理数据并保存offset,Stream.foreachRDD(saveNewOffset)
  //输入funcs可变参数为处理数据（rdd）的方法
  def handleAndSaveOffset(rdd: RDD[ConsumerRecord[String, String]],
                          middleArgsFunc:((RDD[ConsumerRecord[String, String]])=>RDD[(String, String, String, String, List[Double])]),
                          funcs:(RDD[(String, String, String, String, List[Double])]=>Unit)*) = {
    val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //处理rdd
    var tmprdd = middleArgsFunc(rdd)
    for(func<-funcs) {
       func(tmprdd)
    }
    //更新偏移量
    val jedis = JedisConnectionPool.getConnection()
    for(or<-offsetRange) {
      jedis.hset(ConfigManager.getProp("groupId"),or.topic+"-"+or.partition,or.untilOffset.toString)
    }
    jedis.close()
  }
}
