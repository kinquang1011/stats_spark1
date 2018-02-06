package vng.stats.ub.report

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.util.Date
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import util.control.Breaks._
import org.apache.spark.rdd.PairRDDFunctions
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.utils.DateTimeUtils
import vng.stats.ub.utils.Constants

object PlayDurationReport {
  def main(args: Array[String]){
    
    val logDir = args(0)
    val gameCode = args(1)
    val gameType = args(2)  // mobile or pc
    val logDate = args(3)
    val logType = args(4)  // do not need this value at this time
    val timing = args(5)  // a1, a7, a30, daily, weekly, monthly
    var calculateId = args(6)  // unique id using to determine an unique account/device on game, may be: device_id, uid, user_id, open_id, acn
    val inputPath = args(7)
    val outputFileName = args(8)
    var ranges = args(8)
    
    val conf = new SparkConf().setAppName("Play Duration Report")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    
    var calculateMethod = DateTimeUtils.resolveCalculateMethod(timing)
    
    /* load data from parquet file */
    val lstFilePaths = DataUtils.getListFiles(inputPath, logDate, timing);
    val roleTimeRDD = sqlContext.read.parquet(lstFilePaths:_*).rdd.cache
    
    var lstRanges = ranges.split(",").map { x => x.toDouble }
    
    /*val roleTimeRangesRDD = roleTimeRDD.map { row =>  
      
      var gameCode = row.getString(0)
      var logDate = row.getString(1)
      var id = row.getString(2)
      var sid = row.getString(3)
      var rid = row.getString(4)
      var time = row.getDouble(5)
      var timeRanges = "ALL"
      
      timeRanges = determineTimeRange(time, lstRanges)
      
      (gameCode, logDate, id, sid, rid, timeRanges)
    }*/
    
    /**
     * Report role time duration by game server id
     * 
     * group by: gameCode, logDate, sid, timeRanges
     * mapValues: number of roles by time ranges / sid
     * final result: gameCode, logDate, sid, timeRanges, count
     */
    /*val roleTimeRangeCount = roleTimeRangesRDD.groupBy({row => (row._1, row._2, row._4, row._6)}).mapValues(_.size).map(row => 
      (row._1._1,row._1._2,row._1._3,row._1._4,row._2)
    )
    var rolePath = s"""$gslog/$gameCode/ub/report/$logDate/role_duration"""  
    roleTimeRangeCount.toDF().write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", "\t").save(rolePath)*/
    
    /**
     * Calculate time range for each account in a game
     * 
     * map: gameCode, id, time
     * group by: gameCode, id
     * mapValues: total playing time / all sid
     * final result: gameCode, logDate, id, timeRanges
     */
    val accountTimeRangesRDD = roleTimeRDD.map(row => 
      (row.getString(0), row.getString(2), row.getDouble(5))).groupBy({row => 
        (row._1, row._2)}).mapValues(_.map(_._3).sum).map{row => 
          
          var gameCode = row._1._1
          var id = row._1._2
          var time = row._2
          var timeRanges = "ALL"
          
          timeRanges = determineTimeRange(time, lstRanges)
          (gameCode, logDate, id, timeRanges)
        }
    
    /**
     * Report account time duration by game
     * group by: gameCode, logDate, timeRanges
     * mapValues: number of account by time ranges / game
     * final result: gameCode, logDate, timing, id, createDate, sid, timeRanges, count
     */
    val createDate = DateTimeUtils.getDateString(new Date())
    val accountTimeRangesCount = accountTimeRangesRDD.groupBy({row => (row._1, row._2, row._4)}).mapValues(_.size).map(row => 
      (row._1._1,row._1._2, calculateMethod, calculateId, createDate, "all",row._1._3,row._2)
    ).cache


    accountTimeRangesCount.toDF().write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", "\t").save(outputFileName)
    /**
     * Calculate time range for each account in a game
     * 
     * map: gameCode, logDate, id, sid, time
     * group by: gameCode, logDate, id, sid
     * mapValues: total playing time / all sid
     * final result: gameCode, logDate, id, timeRanges
     */
    /*val accountServerTimeRangesRDD = roleTimeRDD.map(row => 
      (row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getDouble(5))).groupBy({row => 
        (row._1, row._2, row._3, row._4)}).mapValues(_.map(_._5).sum).map{row => 
          
          var gameCode = row._1._1
          var logDate = row._1._2
          var id = row._1._3
          var sid = row._1._4
          var time = row._2
          var timeRanges = "ALL"
          
          timeRanges = determineTimeRange(time, lstRanges)
          (gameCode, logDate, id, sid, timeRanges)
        }
    */
    /**
     * Report account time duration by game server id
     * group by: gameCode, logDate, sid, timeRanges
     * mapValues: number of account by time ranges / game server id
     * final result: gameCode, logDate, sid, timeRanges, count
     */
    /*val accountServerTimeRangesCount = accountServerTimeRangesRDD.groupBy({row => (row._1, row._2, row._4, row._5)}).mapValues(_.size).map(row => 
      (row._1._1,row._1._2,calculateValue,calculateId,createDate,row._1._3,row._1._4,row._2)
    )
    var fileName = "server_account_duration" 
    val serverAccountPath = calculateValue match {
      case Constants.DAILY => s"""$gslog/$gameCode/ub/report/daily/$logDate/$fileName"""
      case Constants.WEEKLY => s"""$gslog/$gameCode/ub/report/weekly/$logDate/$fileName"""
      case Constants.MONTHLY => s"""$gslog/$gameCode/ub/report/monthly/$logDate/$fileName"""
      case Constants.A1 => s"""$gslog/$gameCode/ub/report/a1/$logDate/$fileName"""
      case Constants.A7 => s"""$gslog/$gameCode/ub/report/a7/$logDate/$fileName"""
      case Constants.A30 => s"""$gslog/$gameCode/ub/report/a30/$logDate/$fileName"""
    }
    
    accountServerTimeRangesCount.toDF().write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", "\t").save(serverAccountPath)*/
    sc.stop();
  }
  
  private def determineTimeRange(time: Double, lstRanges: Array[Double]): String = {
    
    var min = 0L
    var max = 100000L
    
    var ranges = lstRanges :+ time
    scala.util.Sorting.quickSort(ranges)

    var pos = ranges.lastIndexOf(time)
    if(pos == 0){
      
      min = ranges.apply(0).toLong
      max = ranges.apply(1).toLong
    }else if(pos == ranges.length -1){
      
      min = ranges.apply(pos - 1).toLong
      max = 100000L
    }else{
      
      min = ranges.apply(pos - 1).toLong
      max = ranges.apply(pos + 1).toLong
    }

    min + "x" + max
  }
}