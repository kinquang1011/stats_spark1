package vng.stats.ub.report3

import java.util.Date
import scala.util.Try
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.utils.DateTimeUtils
import vng.stats.ub.report3.collector.CountryDataCollector
import org.apache.spark.sql.functions._
import vng.stats.ub.report3.collector.GroupDataCollector
import vng.stats.ub.report3.collector.OsDataCollector

abstract class BaseReport {

    var sc: SparkContext = _
    
    var parameters: Map[String, String] = Map[String, String]()

    var gameCode = ""
    var logDate = ""
    var timing = ""
    var calcId = ""
    var source = ""
    
    var groupId = ""
    
    var createDate = ""
    var reportDate = ""
    
    var dataCollector: DataCollector = null
    
    /*
     * UDF Area 
     */
    val makeOtherIfNull = udf {(str: String) => 
        if(str == null || str == "") "other" else str
    }
    /* end UDF */
    
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
    }

    final def readDefaultParams(): Unit = {
        
        // required
        gameCode = parameters.get(Constants.Parameters.GAME_CODE).get
        logDate = parameters.get(Constants.Parameters.LOG_DATE).get
        timing = parameters.get(Constants.Parameters.TIMING).get
        calcId = parameters.get(Constants.Parameters.CALC_ID).get
        source = parameters.get(Constants.Parameters.SOURCE).get
        
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
    
    def setParams(params: Map[String, String]): Unit = {
        params.foreach{
        
            mem => {
                var key = mem._1
                var value = mem._2
                
                parameters = parameters + (key -> value)
            }
        }
    }
    
    /* collect data */
    final def collectData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        groupId match {
                
            case Constants.GroupId.GAME => {
                
                
            }
            case Constants.GroupId.SID => {
                
                Common.logger("Using GroupDataCollector")
                dataCollector = new GroupDataCollector(params)
            }
            case Constants.GroupId.CHANNEL => {
                
                Common.logger("Using GroupDataCollector")
                dataCollector = new GroupDataCollector(params)
            }
            case Constants.GroupId.PACKAGE => {
                
                Common.logger("Using GroupDataCollector")
                dataCollector = new GroupDataCollector(params)
            }
            case Constants.GroupId.COUNTRY => {
                
                Common.logger("Using CountryDataCollector")
                dataCollector = new CountryDataCollector(params)
            }
            case Constants.GroupId.OS => {
                
                Common.logger("Using OsDataCollector")
                dataCollector = new OsDataCollector(params)
            }
            case _ => {
                Common.logger("Group Id: " + groupId + " not found")
            }
        }
        
        dataCollector.collect(sqlContext, params)  
    }

    def excute(dataframe: DataFrame): DataFrame

    def store(output: DataFrame): Unit

    /*final def run(args: Array[String]): Unit = {

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
    }*/
    
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
        var df = collectData(sqlContext, params)
        
        if(df != null){
            
            df = excute(df)
            store(df)
        }
    }
    
    final def destroy() {
        sc.stop()
    }
    
    def main(args: Array[String]) {

        //run(args)
    }
}