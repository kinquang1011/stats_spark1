package vng.stats.ub.report

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import java.util.Date
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.utils.DateTimeUtils
import vng.stats.ub.utils.Constants

object KpiPayingReport {
  def main(args: Array[String]){
    
    val logDir = args(0)
    val gameCode = args(1)
    val gameType = args(2)  // mobile or pc
    val logDate = args(3)
    val logType = args(4)  // do not need this value at this time
    var timing = args(5)  // a1, a7, a30, daily, weekly, monthly
    var calculateId = args(6)  // unique id using to determine an unique account/device on game, may be: device_id, uid, user_id, open_id, acn
    val paymentPath = args(7)
    val outputFileName = args(8)
    
    val conf = new SparkConf().setAppName("Kpi Paying Report")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

    var calculateMethod = DateTimeUtils.resolveCalculateMethod(timing)
    /* load data from parquet file */
    val lstFilePaths = DataUtils.getListFilePaths(paymentPath, logDate, calculateMethod, timing);
    val eventDF = sqlContext.read.parquet(lstFilePaths:_*).select("game_code", calculateId, "amount", "first_pay").cache
    
    /* wrong in group by */
    /* have to create one column using for group by */
    /* time_value for timing */
    /* ? for kpi */
    /* solution1: only group by gameCode: easy to implement => report not exactly => used */
    /* solution2: convert logDate to something unique for specific time value (hard to do) => exactly => must be store with 2 time value in mysql*/
    val revenueDF = eventDF.filter("game_code is not null").groupBy("game_code").agg(sum("amount").alias("gross_revenue"), countDistinct(calculateId).alias("paying_user")).cache
    val firstchargeDF = eventDF.where(eventDF("first_pay") > 0).groupBy("game_code").agg(sum("amount").alias("first_revenue"), countDistinct(calculateId).alias("first_user")).cache
    
    val joinDF = revenueDF.as('re).join(firstchargeDF.as('first),
        revenueDF("game_code") === firstchargeDF("game_code"), "left_outer").select("re.game_code","paying_user","gross_revenue","first_user","first_revenue")
    joinDF.registerTempTable("revenue")
    
    var createdDate = DateTimeUtils.getDateString(new Date())
    var createdBy = "spark"
    
    val resultDF = sqlContext.sql(s"""select '$gameType' as game_type, game_code, '$logDate' as log_date, '$calculateMethod' as calculated_by, '$timing' as calculated_value,
        '$calculateId' as calculated_id, '$createdDate' as created_date, '$createdBy' as created_by, paying_user, gross_revenue, first_user, first_revenue from revenue""")
        
    resultDF.write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", "\t").save(outputFileName)
    
    sc.stop();
  }
}