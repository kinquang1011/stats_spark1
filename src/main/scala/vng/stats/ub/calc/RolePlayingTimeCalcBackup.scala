package vng.stats.ub.calc

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.utils.DateTimeUtils

object RolePlayingTimeCalcBackup {
  def main(args: Array[String]){
    
    val logDir = args(0)
    val gameCode = args(1)
    val gameType = args(2)  // mobile or pc
    val logDate = args(3)
    val logType = args(4)  // do not need this value at this time
    val timing = args(5)  // a1, a7, a30, daily, weekly, monthly
    var calculateId = args(6)  // unique id using to determine an unique account/device on game, may be: device_id, uid, user_id, open_id, acn
    val loginlogoutPath = args(7)
    val outputFileName = args(8)
    
    
    val conf = new SparkConf().setAppName("Role Playingtime Calc")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    
    /* load data from parquet file */
    val lstFilePaths = DataUtils.getListFiles(loginlogoutPath, logDate, timing);
    val eventDF = sqlContext.read.parquet(lstFilePaths:_*).select("game_code", calculateId, "sid", "rid", "log_date", "action")
    
    // convert log date & log type in to timestamp & boolean
    val event = eventDF.rdd.map { row =>
    
      var gameCode = row.getString(0)
      var id = row.getString(1)
      var sid = row.getString(2)
      var rid = row.getString(3)
      var logDate = row.getString(4)
      var logType = row.getString(5)
      var timestamp = DateTimeUtils.getTimestamp(logDate)
      var flag = if(logType == "login") 1 else 0
      (gameCode, id, sid, rid, timestamp, flag)
    }
    
    // calculating playing time
    val rolePlayingtime = event.groupBy({ record => (record._1, record._2, record._3, record._4)}).mapValues({values => 
      
      var playtime: Long = 0L
      var timestamp: Long = 0L
      var flag: Int = 0
      var list = values.toList.sortBy(_._5).to[ListBuffer]
      var i = 1
      var listSize = list.size
      var loginTime = 0L
      var logoutTime = DateTimeUtils.getTimestamp(logDate + " 24:00:00")
      while( i < listSize){
        if(list(i-1)._6 == list(i)._6){
          if(list(i)._6 == 1) {
            list.remove(i)
          } else {
            list.remove(i-1)
          }
          i -= 1
          listSize -= 1
        }
        i += 1
      }
      
      // process specific value
      if(list.size > 0 && list(0)._6 == 0){
        list.insert(0, (list(0)._1, list(0)._2, list(0)._3, list(0)._4, DateTimeUtils.getTimestamp(logDate + " 00:00:00"), 1))
      }
      
      if(list.size > 0 && list(list.size - 1)._6 == 1){
        list.append((list(0)._1, list(0)._2, list(0)._3, list(0)._4, DateTimeUtils.getTimestamp(logDate + " 24:00:00"), 0))
      }
      
      // calculating
      var playingtime = 0L
      listSize  = list.size - 1
      for(j <- 0 to listSize by 2){
        loginTime = list(j)._5
        logoutTime = list(j+1)._5
        playingtime += (logoutTime - loginTime)
      }
      (playingtime * 1.0)/ (60 * 1000)
    })
    
    rolePlayingtime.toDF().registerTempTable("role_playingtime")
    val rolePlayingtimeDF = sqlContext.sql(s"""select _1._1 as game_code, '$logDate' as log_date, _1._2 as $calculateId, _1._3 as sid, _1._4 as rid, _2 as time from role_playingtime""")
    rolePlayingtimeDF.write.mode("overwrite").parquet(outputFileName)
    
    rolePlayingtime.unpersist(false)
    sc.stop()
  }
}