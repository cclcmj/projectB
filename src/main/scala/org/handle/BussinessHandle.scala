package org.handle

import org.Utils.{JDBCUtiles, JedisConnectionPool, TimeSubUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.util.parsing.json.JSON

/**
  * 处理数据的方法
  */
object BussinessHandle {
  //输出测试
  def printest(rdd: RDD[ConsumerRecord[String, String]]): Unit = {
    val resrdd = rdd.asInstanceOf[RDD[ConsumerRecord[String, String]]]
    resrdd.map(_.value()).foreach(println)
  }
  //将stream转换为需要的参数（天，时，分钟，省份，List（订单数，充值金额，充值成功数，时长））
  def middleArgsRDD(cityMap:Broadcast[Map[String,String]], initrdd: RDD[ConsumerRecord[String, String]]): RDD[(String, String, String, String, List[Double])] ={
    //解析JSON
    initrdd.map(_.value()).map(t=>JSON.parseFull(t).get.asInstanceOf[Map[String,Any]])
    //过滤需要的数据
      .filter(map=>map.get("serviceName").get.toString.equalsIgnoreCase("reChargeNotifyReq"))
    //转换为需要的几个参数
      .map(map=>{
      val result = map.get("bussinessRst").get.toString //充值结果
      val money : Double = if(result.equals("0000")) map.get("chargefee").get.toString.toDouble else 0.0 // 充值金额
      val feecount = if(result.equals("0000")) 1 else 0 //充值成功数
      val starttime = map.get("requestId").get.toString //开始充值时间
      val stoptime = map.get("receiveNotifyTime").get.toString //结束充值时间
      val province = cityMap.value.get(map.get("provinceCode").get.toString).get//省份
      val duration = TimeSubUtils.getDuration(starttime,stoptime)
      (starttime.substring(0,8),starttime.substring(0,10),starttime.substring(0,12),province,List[Double](1,money,feecount,duration))
    }).cache()
  }
  def result01_01(lines:RDD[(String, String, String, String, List[Double])])= {
     lines.map(data=>(data._1,data._5))
       .reduceByKey((list1,list2)=>list1.zip(list2).map(t=>t._1+t._2))
       .foreachPartition(f=>{
      val jedis = JedisConnectionPool.getConnection()
      f.foreach(t=>{
        //充值订单数
        jedis.hincrBy(t._1,"count",t._2(0).toLong)
        //充值金额
        jedis.hincrByFloat(t._1,"money",t._2(1))
        //充值成功书
        jedis.hincrBy(t._1,"success",t._2(2).toLong)
      })
    })
  }
  def result01_02(lines:RDD[(String, String, String, String, List[Double])]) = {
    lines.map(data=>(data._3,data._5(0)))
      .reduceByKey((a,b)=>a+b)
      .foreachPartition(f=>{
        val jedis = JedisConnectionPool.getConnection()
        f.foreach(t=>{
          jedis.hincrBy("20170412minute",t._1,t._2.toLong)
        })
        jedis.close()
      })
  }
  def result02(lines:RDD[(String, String, String, String, List[Double])]) = {
    lines.map(data=>((data._2,data._4),List[Double](data._5(0),data._5(2))))
      .reduceByKey((list1,list2)=>list1.zip(list2).map(t=>t._1+t._2))
      .map(res=>(res._1._1.toString+res._1._2.toString,res._2(0)-res._2(1)))
      .foreachPartition(f=>{
        val jedis = JedisConnectionPool.getConnection()
        f.foreach(t=>{
          jedis.hincrBy("20170412hour",t._1,t._2.toLong)
        }
        )
        jedis.close()
      })
  }

  /**
    * 以省份为维度统计每小时订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数，存入MySQL中，进行前台页面显示。
    * @param lines
    * @return
    */
  def result03(lines:RDD[(String, String, String, String, List[Double])]) = {

    lines.map(data=>((data._2,data._4),List[Double](data._5(0),data._5(2))))
      .reduceByKey((list1,list2)=>list1.zip(list2).map(t=>t._1+t._2))
      .map(data=>(data._1._1,(data._1._2,data._2(0),data._2(1)/data._2(0))))
      .groupByKey()
      .map(data=>(data._1,data._2.toList.sortBy(t=>t._2).map(t=>(t._1,t._3)).take(10)))
      .foreachPartition(f=>{
        val conn = JDBCUtiles.getConn
        val stat = conn.createStatement()
        //保存（时间，List（（省份，成功率），（）））
        var sql = "INSERT INTO result03 (hourtime  ,pro_one ,succrate_one ,pro_two ,succrate_two ,pro_three ,succrate_three ,pro_four ,succrate_four ,pro_five ,succrate_five ,pro_six ,succrate_six ,pro_seven ,succrate_seven ,pro_eight ,succrate_eight ,pro_nine ,succrate_nine,pro_ten,succrate_ten ) VALUES ("
          f.foreach(lines=>{
            sql += lines._1
            for(elem<-lines._2) {
              sql += ",'"+elem._1+"',"+elem._2
            }
            sql += ")"
            println(sql)
            stat.execute(sql)
          })
        JDBCUtiles.closeConn(conn)
      })
  }
  /**
    * 实时统计每小时的充值笔数和充值金额
    */
  def result04(lines:RDD[(String, String, String, String, List[Double])]) = {
    lines.map(data=>(data._2,List[Double](data._5(2),data._5(1))))
      .reduceByKey((list1,list2)=>list1.zip(list2).map(t=>t._1+t._2))
      .foreachPartition(f=>{
        //保存（时间，List（成功数，金额））
      })
  }
}
//object Test extends App{
//  val conn = JDBCUtiles.getConn
//  val stat = conn.createStatement()
//  stat.execute("INSERT INTO result03 (hourtime  ,pro_one ,succrate_one ,pro_two ,succrate_two ,pro_three ,succrate_three ,pro_four ,succrate_four ,pro_five ,succrate_five ,pro_six ,succrate_six ,pro_seven ,succrate_seven ,pro_eight ,succrate_eight ,pro_nine ,succrate_nine,pro_ten,succrate_ten ) VALUES (2017041206,'新疆',1.0,'云南',1.0,'贵州',1.0,'天津',1.0,'上海',1.0,'青海',1.0,'浙江',1.0,'安徽',1.0,'甘肃',1.0,'福建',1.0)")
//  conn.close()
//}