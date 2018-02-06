package vng.stats.ub.report3.collector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import vng.stats.ub.report3.DataCollector
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils
import org.apache.spark.sql.functions._

class GroupDataCollector(params: Map[String, String]) extends DataCollector(params) {

    override def collectActivityData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        var activityPath = params(Constants.Parameters.ACTIVITY_PATH)
        
        var lstActivityFiles = DataUtils.getListFiles(activityPath, logDate, timing);
        var activityDF = sqlContext.read.option("mergeSchema", "true").parquet(lstActivityFiles: _*).select(groupId, calcId).distinct
        
        var joinDF = activityDF.select(groupId, calcId)
        //joinDF.show
        Common.logger("Group: activity data")
        joinDF
    }
    
    override def collectAccRegisterData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        var regPath = params(Constants.Parameters.ACC_REGISTER_PATH)
        
        var lstRegFiles = DataUtils.getListFiles(regPath, logDate, timing);
        var regDF = sqlContext.read.option("mergeSchema", "true").parquet(lstRegFiles: _*).select(groupId, calcId).distinct
        
        var joinDF = regDF.select(groupId, calcId).distinct
        //joinDF.show
        Common.logger("Group: accregister data")
        joinDF
    }
    
    override def collectPaymentData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        var paymentPath = params(Constants.Parameters.PAYMENT_PATH)
        
        var lstPaymentFiles = DataUtils.getListFiles(paymentPath, logDate, timing);
        var paymentDF = sqlContext.read.option("mergeSchema", "true").parquet(lstPaymentFiles: _*)
        
        var joinDF = paymentDF.select(groupId, calcId, "net_amt", "gross_amt")
        //joinDF.show
        Common.logger("Group: payment data")
        joinDF
    }
    
    override def collectFirstChargeData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {

        var paymentPath = params(Constants.Parameters.PAYMENT_PATH)
        var firstChargePath = params(Constants.Parameters.FIRSTCHARGE_PATH)
        
        var lstPaymentFiles = DataUtils.getListFiles(paymentPath, logDate, timing);
        var paymentDF = sqlContext.read.option("mergeSchema", "true").parquet(lstPaymentFiles: _*)
        
        var lstFirstChargeFiles = DataUtils.getListFiles(firstChargePath, logDate, timing);
        var firstDF = sqlContext.read.option("mergeSchema", "true").parquet(lstFirstChargeFiles: _*)
        
        var joinDF = paymentDF.join(firstDF, 
                paymentDF(calcId) === firstDF(calcId), "leftsemi")    // do not need to join on group id because firstcharge is calculated on payment
                .select(groupId, calcId, "net_amt", "gross_amt")
        //joinDF.show
        Common.logger("Group: firstcharge data")
        joinDF
    }
    
    def collectNewUserRevenueData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        var paymentPath = params(Constants.Parameters.PAYMENT_PATH)
        var regPath = params(Constants.Parameters.ACC_REGISTER_PATH)
        
        var lstRegFiles = DataUtils.getListFiles(regPath, logDate, timing);
        var regDF = sqlContext.read.option("mergeSchema", "true").parquet(lstRegFiles: _*).select("game_code", calcId).distinct
        
        var lstPaymentFiles = DataUtils.getListFiles(paymentPath, logDate, timing);
        var paymentDF = sqlContext.read.option("mergeSchema", "true").parquet(lstPaymentFiles: _*)
                
        var joinDF = paymentDF.join(regDF, paymentDF(calcId) === regDF(calcId), "leftsemi")    // #collectFirstChargeData
        //joinDF.show
        
        Common.logger("Group: new user revenue data")
        joinDF
    }
    
    def collectNewUserRetentionData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        var activityPath = params(Constants.Parameters.ACTIVITY_PATH)
        var regPath = params(Constants.Parameters.ACC_REGISTER_PATH)
        
        var regFile = DataUtils.getFile(regPath, logDate, timing);
        
        if (!DataUtils.isEmpty(regFile)) {
            var regDF = sqlContext.read.option("mergeSchema", "true").parquet(regFile).select(groupId, calcId)
            
            var activityFile = DataUtils.getFile(activityPath, logDate);
            var activityDF = sqlContext.read.option("mergeSchema", "true").parquet(activityFile).select(groupId, calcId).distinct

            var joinDF = regDF.as('pr).join(activityDF.as('ca), 
                    regDF(calcId) === activityDF(calcId) && regDF(groupId) === activityDF(groupId), "left_outer")
                    .selectExpr("pr." + calcId + " as pr" + calcId, "ca." + calcId + " as ca" + calcId, "pr." + groupId)
            //joinDF.show
            
            Common.logger("Group: new user retention data")
            joinDF
        } else {
            
            // if return null, excution will stop here
            Common.logger("Group: new user retention data - " + regFile + " not existed!")
            null
        }
    }
    
    def collectUserRetentionData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        var activityPath = params(Constants.Parameters.ACTIVITY_PATH)
        
        var prevActivityFile = DataUtils.getFile(activityPath, logDate, timing);
        
        if (!DataUtils.isEmpty(prevActivityFile)) {
            
            var prevActivityDF = sqlContext.read.option("mergeSchema", "true").parquet(prevActivityFile).select(groupId, calcId).distinct
            
            var activityFile = DataUtils.getFile(activityPath, logDate);
            var activityDF = sqlContext.read.option("mergeSchema", "true").parquet(activityFile).select(groupId, calcId).distinct
            
            var joinDF = prevActivityDF.as('pa).join(activityDF.as('ca), 
                    prevActivityDF(calcId) === activityDF(calcId) && prevActivityDF(groupId) === activityDF(groupId), "left_outer")
                    .selectExpr("pa." + calcId + " as pa" + calcId,  "ca." + calcId + " as ca" + calcId,"pa." + groupId)
            //joinDF.show
            
            Common.logger("Group: user retention data")
            joinDF
        } else {
            
            // if return null, excution will stop here 
            Common.logger("Group: user retention data - " + prevActivityFile + " not existed!")
            null
        }
    }
    
    def collectFirstChargeRetentionData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        var payPath = params(Constants.Parameters.PAYMENT_PATH)
        var firstPayPath = params(Constants.Parameters.FIRSTCHARGE_PATH)
        
        var firstFile = DataUtils.getFile(firstPayPath, logDate, timing);
        
        if (!DataUtils.isEmpty(firstFile)) {
            
            var firstDF = sqlContext.read.option("mergeSchema", "true").parquet(firstFile).select(groupId, calcId)
            
            var payFile = DataUtils.getFile(payPath, logDate);
            var payDF = sqlContext.read.option("mergeSchema", "true").parquet(payFile).select(groupId, calcId).distinct
            
            var joinDF = firstDF.as('pp).join(payDF.as('cp), 
                    firstDF(calcId) === payDF(calcId) && firstDF(groupId) === payDF(groupId), "left_outer")
                    .selectExpr("pp." + calcId + " as pp" + calcId,  "cp." + calcId + " as cp" + calcId, "pp." + groupId)
            //joinDF.show
            
            Common.logger("Group: firstcharge retention data")
            joinDF
        } else {
            
            // if return null, excution will stop here
            Common.logger("Group: firstcharge retention data - " + firstFile + " not existed!")
            null
        }
    }
    
    override def collectCcuData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        if(timing != "a1"){
            
            Common.logger("Group: ccu can only run daily!")
            null
        }
        
        var ccuPath = params(Constants.Parameters.CCU_PATH)
        var lstLogFiles = DataUtils.getListFiles(ccuPath, logDate, timing);
        var logDF = sqlContext.read.option("mergeSchema", "true").parquet(lstLogFiles: _*)
        
        var ccuDF = logDF.select(groupId, "ccu")
        
        Common.logger("Group: ccu data")
        ccuDF
    }
}