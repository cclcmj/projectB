package org.Utils

import java.util.Properties

/**
  * 获取kafka的配置信息
  * 包括需要的序列化类、kafka所在的服务器、redis所在的服务器
  */
object ConfigManager {
  val prop = new Properties()
  try{
    val in_kafkaconfig = ConfigManager.getClass.getClassLoader.getResourceAsStream("Config.properties")
    prop.load(in_kafkaconfig)
  }catch {
    case e:Exception=>e.printStackTrace()
  }
  def getProp(key:String):String={
    prop.getProperty(key)
  }
}