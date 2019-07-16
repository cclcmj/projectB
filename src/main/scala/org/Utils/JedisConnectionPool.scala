package org.Utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Jedis连接池
  * 用来连接redis
  */
object JedisConnectionPool {
  val config=new JedisPoolConfig
  //设置最大连接数
  config.setMaxTotal(30)
  //设置最大空闲数
  config.setMaxIdle(10)
  //创建池
  val pool = new JedisPool(config,
    ConfigManager.getProp("redis.hostname"),
    ConfigManager.getProp("redis.port").toInt,
    ConfigManager.getProp("redis.timeout").toInt,
    ConfigManager.getProp("redis.auth"))
  //获取Jedis对象
  def getConnection():Jedis={
    pool.getResource
  }
}
