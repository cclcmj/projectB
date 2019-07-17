package org.Utils

import java.util.Properties

object JDBCUtiles {
  def getJdbcProp = {
    val prop = new Properties()
    prop.put("user",ConfigManager.getProp("jdbc.user"))
    prop.put("password",ConfigManager.getProp("jdbc.password"))
    prop.put("driver",ConfigManager.getProp("jdbc.driver"))
    val jdbcUrl = ConfigManager.getProp("jdbc.url")
    (prop,jdbcUrl)
  }
}
