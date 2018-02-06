package vng.stats.ub.report3

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils
import scala.util.Try

abstract class DataCollector(params: Map[String, String]) {
    
    /* scala default constructor */
    var gameCode = params(Constants.Parameters.GAME_CODE)
    var logDate = params(Constants.Parameters.LOG_DATE)
    var timing = params(Constants.Parameters.TIMING)
    var calcId = params(Constants.Parameters.CALC_ID)
    var source = params(Constants.Parameters.SOURCE)
    var groupId = ""
    
    var isExist = Try {
    
        groupId = params(Constants.Parameters.GROUP_ID)
    }
    /* end default constructor */
    
    
    final def collect(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        var reportNumber = params(Constants.Parameters.REPORT_NUMBER)
        
        reportNumber match {
                case Constants.ReportNumber.CCU => {
                    collectCcuData(sqlContext, params)
                }
                case Constants.ReportNumber.ACCOUNT_REGISTER => {
                    collectAccRegisterData(sqlContext, params)
                }
                case Constants.ReportNumber.ACTIVE_USER => {
                    collectActivityData(sqlContext, params)
                }
                case Constants.ReportNumber.USER_RETENTION => {
                    collectUserRetentionData(sqlContext, params)                    
                }
                case Constants.ReportNumber.NEWUSER_RETENTION => {
                    collectNewUserRetentionData(sqlContext, params)
                }
                case Constants.ReportNumber.NEWUSER_REVENUE => {
                    collectNewUserRevenueData(sqlContext, params)
                }
                case Constants.ReportNumber.REVENUE => {
                    collectPaymentData(sqlContext, params)
                }
                case Constants.ReportNumber.FIRST_CHARGE => {
                    collectFirstChargeData(sqlContext, params)
                }
                case Constants.ReportNumber.FIRST_CHARGE_RETENTION => {
                    collectFirstChargeRetentionData(sqlContext, params)
                }
        }
    }
    
    protected def collectActivityData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        var gameCode = params(Constants.Parameters.GAME_CODE)
        var logDate = params(Constants.Parameters.LOG_DATE)
        var timing = params(Constants.Parameters.TIMING)
        var calcId = params(Constants.Parameters.CALC_ID)
        var source = params(Constants.Parameters.SOURCE)
        
        var activityPath = params(Constants.Parameters.ACTIVITY_PATH)
        var lstActivityFiles = DataUtils.getListFiles(activityPath, logDate, timing);
        var activityDF = sqlContext.read.parquet(lstActivityFiles: _*)
        
        activityDF
    }
    
    def collectPaymentData(sqlContext: SQLContext, params: Map[String, String]): DataFrame
    def collectAccRegisterData(sqlContext: SQLContext, params: Map[String, String]): DataFrame
    def collectFirstChargeData(sqlContext: SQLContext, params: Map[String, String]): DataFrame
    def collectNewUserRevenueData(sqlContext: SQLContext, params: Map[String, String]): DataFrame
    def collectNewUserRetentionData(sqlContext: SQLContext, params: Map[String, String]): DataFrame
    def collectUserRetentionData(sqlContext: SQLContext, params: Map[String, String]): DataFrame
    def collectFirstChargeRetentionData(sqlContext: SQLContext, params: Map[String, String]): DataFrame
    def collectCcuData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        null
    }
    
    /*
    def collectPayingData(sqlContext: SQLContext, params: Map[String, String]): DataFrame
    def collectCcuData(sqlContext: SQLContext, params: Map[String, String]): DataFrame
    */
}