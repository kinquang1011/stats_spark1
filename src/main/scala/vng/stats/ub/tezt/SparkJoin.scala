package vng.stats.ub.tezt

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.broadcast

/*
 * ~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-shell --master yarn --queue production --executor-memory 4g --num-executors 2 --driver-memory 2g
 * 
 * */
object SparkJoin {

    def main(agrs: Array[String]) {
        
        val scheduleMode = agrs(0)
        val date = agrs(1)
        val wait = agrs(2)
        var mode = "FIFO"
        
        if(scheduleMode == "FAIR"){
            mode = scheduleMode
        }
        
        val conf = new SparkConf().setAppName("Spark Join")
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        conf.set("spark.scheduler.mode", mode)
        conf.set("spark.locality.wait", wait)

        var sc = new SparkContext(conf)

        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
        import sqlContext.implicits._
        
        val activeDF = sqlContext.read.parquet("/ge/warehouse/3qmobile/ub/data/activity_2/" + date)
        val payDF = sqlContext.read.parquet("/ge/warehouse/3qmobile/ub/data/payment_2/" + date)
        
        val finalDF = activeDF.as('a).join(payDF.as('p), activeDF("id") === payDF("id"), "left_outer")
        val count = finalDF.where("p.id is not null").count
        
        println(count)
    }
}