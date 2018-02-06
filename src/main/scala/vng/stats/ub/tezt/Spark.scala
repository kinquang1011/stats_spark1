package vng.stats.ub.tezt

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/*
 * ~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-shell --master yarn --queue production --executor-memory 4g --num-executors 2 --driver-memory 2g
 * 
 * */
object Spark {

    def main(agrs: Array[String]) {
        
        val scheduleMode = agrs(0)
        var mode = "FIFO"
        
        if(scheduleMode == "FAIR"){
            mode = scheduleMode
        }
        
        val conf = new SparkConf().setAppName("Spark Test")
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        conf.set("spark.scheduler.mode", mode)
        conf.set("spark.locality.wait", "120s")
        
        var sc = new SparkContext(conf)

        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
        import sqlContext.implicits._
        
        val count = sqlContext.read.parquet("/ge/warehouse/3qmobile/ub/data/activity_2/2016-*").count
        println("VDP: " + count)
    }
}