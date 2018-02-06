package vng.stats.ub.report

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class Report (name: String) {
  
  val sc: SparkContext = {
      
      val conf = new SparkConf().setAppName(name)
      conf.set("spark.hadoop.validateOutputSpecs", "false")
      
      new SparkContext(conf)
  }
  
  val sqlContext: SQLContext = {
      
      val sqlContext = new SQLContext(sc)
      sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
      sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
      import sqlContext.implicits._
      sqlContext
  }
  
  def destroy() {
    sc.stop()
  }
}