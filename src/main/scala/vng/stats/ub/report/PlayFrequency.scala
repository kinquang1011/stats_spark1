package vng.stats.ub.report

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.util.Date
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import util.control.Breaks._
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.utils.DateTimeUtils

case class Frequency(gameCode:String, logDate:String, timing:String, frequency:String, group: Long, total: Long)

object PlayFrequency {
  def main(args: Array[String]){
    
    val inputPath = args(0) //hdfs folder log. ex: /ge/warehouse/[game_code]/[log_type]
    val gameCode = args(1)
    val logDate = args(2)
    val calculatedBy = args(3) // daily, weekly, monthly
    val outputPath = args(4) // daily, weekly, monthly
     val conf = new SparkConf().setAppName("PlayFrequency")
    val sc = new SparkContext(conf)
     val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    val lstFilePaths = DataUtils.getListFiles(inputPath, logDate, calculatedBy);
    
    val events = sqlContext.read.parquet(lstFilePaths:_*)
    
    // val events = sqlContext.read.parquet("/ge/warehouse/contra/ub/data/2016-01-01/event_login")
    //events.show()
    val eDF = events.toDF()
    
    eDF.registerTempTable("events")
    val eventLogins = sqlContext.sql("select * from events where action='login'")
    //eventLogins.select("id", "log_date")
    val loginTicks = eventLogins.select("id", "log_date").rdd.map { row =>
      var id = row.getString(0)
      var logDate= DateTimeUtils.getLogDate(row.getString(1));
      (id, logDate)
    }
     loginTicks.cache()
    val loginSessionTimes = loginTicks.groupBy({ r => (r._1)}).mapValues({values =>
        (values.size.toLong)
    })
    val loginDayTimes = loginTicks.groupBy({ r => (r._1)}).mapValues({values =>
        val v = scala.collection.mutable.Map.empty[String,String]
        values.foreach { x =>
          ( v (x._2) = x._1)
        }
        (v.size.toLong)
    })
    
    val loginSessionGroups = loginSessionTimes.map { row =>
      var group = row._2
      (group,  1L)
    }
    
    val loginDayGroups = loginDayTimes.map { row =>
      var group = row._2
      (group,  1L)
    }
    
     val loginSessionCounterGroups = loginSessionGroups.reduceByKey(_ + _).sortBy(x=>x._2,false)
     
     val loginDayCounterGroups = loginDayGroups.reduceByKey(_ + _).sortBy(x=>x._2,false)

    val arrs = scala.collection.mutable.ArrayBuffer.empty[Frequency]
     
    loginSessionCounterGroups.collect().foreach((x)=>{
            arrs += new Frequency(gameCode, logDate, calculatedBy,"session", x._1,x._2)
        }
        )
     if(calculatedBy!="daily"){
        loginDayCounterGroups.collect().foreach((x)=>{
            arrs += new Frequency(gameCode, logDate, calculatedBy,"day", x._1,x._2)
        }
        )
     }
    val storeValues = sc.parallelize(arrs)
    val df = sqlContext.createDataFrame(storeValues)
    
    df.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", "\t").save(outputPath)
    arrs.clear();
    loginTicks.unpersist(false)
    sc.stop();
  }
}