package org.Utils

import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.HasOffsetRanges

/**
  * 获取提交偏移量
  */
case class redisOffsetUtils() {
  //判断是否有访问过该topic
  def isFirst(fromdbOffset:Map[TopicPartition,Long]): Boolean = {
    fromdbOffset.size==0
  }
  //获取offset
  def getOffset: Map[TopicPartition, Long] = {
    //获取jedis连接
    val jedis = JedisConnectionPool.getConnection()
    //获取所有的Topic、Partition
    val topicPartitionOffset: util.Map[String, String] = jedis.hgetAll(ConfigManager.getProp("groupId"))
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
  //处理数据后的保存offset,Stream.foreachRDD(saveNewOffset)
  def saveNewOffset[T<:Any](rdd: RDD[T]) = {
    val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //更新偏移量
    val jedis = JedisConnectionPool.getConnection()
    for(or<-offsetRange) {
      jedis.hset(ConfigManager.getProp("groupId"),or.topic+"-"+or.partition,or.untilOffset.toString)
    }
    jedis.close()
  }
}
