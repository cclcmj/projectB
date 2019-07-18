package org.Utils

import java.sql.{Connection, DriverManager}


object JDBCUtiles {
  def getConn: Connection = {
    Class.forName(ConfigManager.getProp("jdbc.driver"))
    DriverManager.getConnection(ConfigManager.getProp("jdbc.url"),ConfigManager.getProp("jdbc.user"),ConfigManager.getProp("jdbc.password"))
  }
  def closeConn(conn: Connection) = {
    try{
      conn.close()
    }catch {
      case e: Exception=>{
        e.printStackTrace()
      }
    }
  }
}
