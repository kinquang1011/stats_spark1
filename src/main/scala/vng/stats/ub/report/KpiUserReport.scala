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
import vng.stats.ub.utils.DateTimeUtils
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.utils.Constants

case class UserKpiReport(gameType: String, gameCode: String, logDate: String, calculatedBy: String, calculatedValue: String, calculateId: String, createdDate: String, createBy: String,
    active: Long, retention: Long, churn: Long, newRegister: Long, roleRegister: Long)

object KpiUserReport {
  def main(args: Array[String]){
    
    val logDir = args(0)
    val gameCode = args(1)
    val gameType = args(2)  // mobile or pc
    val logDate = args(3)
    val logType = args(4)  // do not need this value at this time
    val timing = args(5)  // a1, a7, a30, daily, weekly, monthly
    var calculateId = args(6)  // unique id using to determine an unique account/device on game, may be: device_id, uid, user_id, open_id, acn
    val loginlogoutPath = args(7)
    val accRegisterPath = args(8)
    val roleRegisterPath = args(9)
    val outputFileName = args(10)
    
    val conf = new SparkConf().setAppName("Kpi User Report")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    
    var calculateMethod = DateTimeUtils.resolveCalculateMethod(timing)
    /* load data from parquet file */
    val lstFilePaths = DataUtils.getListFilePaths(loginlogoutPath, logDate, calculateMethod, timing);
    val eventDF = sqlContext.read.parquet(lstFilePaths:_*)
    
    /* calculate active user*/
    val dUserDF = eventDF.select(calculateId).distinct().cache
    var active = dUserDF.count
    
    var eventLoginSchema = StructType(
        StructField("game_code",StringType,true) :: 
        StructField("action",StringType,true) ::
        StructField("log_date",StringType,true) ::
        StructField("uid",StringType,true) ::
        StructField("user_id",StringType,true) ::
        StructField("open_id",StringType,true) ::
        StructField("acn",StringType,true) ::
        StructField("sid",StringType,true) ::
        StructField("rid",StringType,true) ::
        StructField("rnm",StringType,true) ::
        StructField("online_time",StringType,true) ::
        StructField("amount",StringType,true) ::
        StructField("level",StringType,true) ::
        StructField("ip",StringType,true) ::
        StructField("device_id",StringType,true) :: Nil
    )
    var previousEventDF = sqlContext.createDataFrame(sc.emptyRDD[Row], eventLoginSchema)
    
    /* load previous data from parquet file */
    val lstFilePathsPrevious1Times = DataUtils.getListFilePathsBetweenNTimes(loginlogoutPath, logDate, calculateMethod, timing, -1)  /* load previous date */
    val fs = FileSystem.get(new Configuration(true));
    
    if(!DataUtils.isEmpty(lstFilePathsPrevious1Times)){
      
      previousEventDF = sqlContext.read.parquet(lstFilePathsPrevious1Times:_*)
    }
    
    /* calculate retention */
    val dPreviousUserDF = previousEventDF.select(calculateId).distinct().cache
    val joinDF = dPreviousUserDF.join(dUserDF, dPreviousUserDF(calculateId) === dUserDF(calculateId))
    var retention = joinDF.count
    var total = dPreviousUserDF.count
    
    var accRegSchema = StructType(
        StructField("game_code",StringType,true) :: 
        StructField("log_date",StringType,true) ::
        StructField("uid",StringType,true) ::
        StructField("user_id",StringType,true) ::
        StructField("open_id",StringType,true) ::
        StructField("acn",StringType,true) ::
        StructField("device_id",StringType,true) :: Nil
    )
    
    var roleRegSchema = StructType(
        StructField("game_code",StringType,true) :: 
        StructField("log_date",StringType,true) ::
        StructField("uid",StringType,true) ::
        StructField("user_id",StringType,true) ::
        StructField("open_id",StringType,true) ::
        StructField("acn",StringType,true) ::
        StructField("sid",StringType,true) ::
        StructField("rid",StringType,true) ::
        StructField("device_id",StringType,true) :: Nil
    )
    
    var eventRegisterDF = sqlContext.createDataFrame(sc.emptyRDD[Row], accRegSchema)
    var roleRegisterDF = sqlContext.createDataFrame(sc.emptyRDD[Row], roleRegSchema)
    
    var newRegister = 0L
    var roleRegister = 0L
    
    /* calculate new register user all time */
    val lstRegisterPath = DataUtils.getListFilePaths(accRegisterPath, logDate, calculateMethod, timing);  /* account register */
    val lstRoleRegisterPath = DataUtils.getListFilePaths(roleRegisterPath, logDate, calculateMethod, timing); /* role_register */
    
    if(!DataUtils.isEmpty(lstRegisterPath)){
      
      eventRegisterDF = sqlContext.read.parquet(lstRegisterPath:_*)
      newRegister = eventRegisterDF.select(calculateId).distinct().count()
    }
    
    if(!DataUtils.isEmpty(lstRoleRegisterPath)){
      
      roleRegisterDF = sqlContext.read.parquet(lstRoleRegisterPath:_*)
      roleRegister = roleRegisterDF.select("sid", "rid").distinct().count()
    }
    
    val createDate = DateTimeUtils.getDateString(new Date())
    val createBy = "spark"
    
    val x = sc.parallelize(Array(UserKpiReport(gameType, gameCode, logDate, calculateMethod, timing, calculateId, createDate, createBy,
        active, retention, total - retention, newRegister, roleRegister)))
    val df = sqlContext.createDataFrame(x)
    df.write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", "\t").save(outputFileName)
    
    /* release resources */
    dUserDF.unpersist()
    dPreviousUserDF.unpersist()
    sc.stop();
  }
}