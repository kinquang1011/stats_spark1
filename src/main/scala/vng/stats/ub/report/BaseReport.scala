package vng.stats.ub.report

import scala.reflect.runtime.universe
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils
import java.util.Date
import vng.stats.ub.utils.DateTimeUtils
import org.apache.spark.sql.DataFrame
import scala.util.Try
import vng.stats.ub.utils.Common

abstract class BaseReport {

    var sc: SparkContext = _
    
    var parameters: Map[String, String] = Map[String, String]()

    var gameCode = ""
    var logDate = ""
    var timing = ""
    var calcId = ""
    var source = ""
    var inputPath = ""
    var outputPath = ""
    
    var groupId = ""
    
    var createDate = ""
    var reportDate = ""
    
    final def initSparkContext(sparkContext: SparkContext): Unit = {
        
        sc = sparkContext
    }
    
    final def init(args: Array[String]): Unit = {
        sc = {

            val conf = new SparkConf().setAppName("")
            conf.set("spark.hadoop.validateOutputSpecs", "false")
    
            new SparkContext(conf)
        }
        parameters = DataUtils.getParameters(args)
        readDefaultParams()
        readExtraParams()
    }

    final def readDefaultParams(): Unit = {
        
        // required
        gameCode = parameters.get(Constants.Parameters.GAME_CODE).get
        logDate = parameters.get(Constants.Parameters.LOG_DATE).get
        timing = parameters.get(Constants.Parameters.TIMING).get
        calcId = parameters.get(Constants.Parameters.CALC_ID).get
        source = parameters.get(Constants.Parameters.SOURCE).get
        inputPath = parameters.get(Constants.Parameters.INPUT_PATH).get
        outputPath = parameters.get(Constants.Parameters.OUTPUT_PATH).get
        
        // optional by report type
        var isExist = Try {
            groupId = parameters(Constants.Parameters.GROUP_ID)
        }
        
        if(isExist.isFailure){
            
            Common.logger("GroupID Not Found")
        }
        
        createDate = DateTimeUtils.getDateString(new Date())
        reportDate = DataUtils.formatReportDate(logDate)
    }
    
    def readExtraParams(): Unit = {
        
    }
    
    def setParams(params: Map[String, String]): Unit = {
        params.foreach{
        
            mem => {
                var key = mem._1
                var value = mem._2
                
                parameters = parameters + (key -> value)
            }
        }
    }

    def excute(sqlContext: SQLContext): DataFrame

    def write(df: DataFrame): Unit = {

        //df.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", "\t").save(outputPath + "/" + logDate)
    }

    final def run(args: Array[String]): Unit = {

        init(args)
        
        val sqlContext: SQLContext =  {

            val sqlContext = new SQLContext(sc)
            sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
            sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
            import sqlContext.implicits._
            
            sqlContext
        }
        
        var df = excute(sqlContext)
        write(df)
        destroy()
    }
    
    final def rerun(params: Map[String, String], sc: SparkContext): Unit = {
        
        initSparkContext(sc)
        
        val sqlContext: SQLContext =  {

            val sqlContext = new SQLContext(sc)
            sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
            sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
            import sqlContext.implicits._
            
            sqlContext
        }
        
        setParams(params)
        readDefaultParams()
        readExtraParams()
        var df = excute(sqlContext)
        write(df)
    }
    
    final def destroy() {
        sc.stop()
    }
    
    def main(args: Array[String]) {

        run(args)
    }
}