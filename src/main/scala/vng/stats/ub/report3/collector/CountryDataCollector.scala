package vng.stats.ub.report3.collector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import vng.stats.ub.report3.DataCollector
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils

class CountryDataCollector(params: Map[String, String]) extends DataCollector(params) {

    override def collectActivityData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        var activityPath = params(Constants.Parameters.ACTIVITY_PATH)
        var countryPath = params(Constants.Parameters.MAPPING_COUNTRY)
        
        var lstActivityFiles = DataUtils.getListFiles(activityPath, logDate, timing);
        var activityDF = sqlContext.read.option("mergeSchema", "true").parquet(lstActivityFiles: _*).select("game_code", calcId).distinct
        
        var lstMappingFiles = DataUtils.getListFiles(countryPath, logDate, timing);
        var countryDF = sqlContext.read.option("mergeSchema", "true").parquet(lstMappingFiles: _*).sort("log_date").dropDuplicates(Seq(calcId)).select(calcId,"country_code")
        
        var joinDF = activityDF.as('a).join(countryDF.as('c), 
                activityDF(calcId) === countryDF(calcId), "left_outer")
                .select("a.game_code","a." + calcId, "c.country_code")
        //joinDF.show
        Common.logger("Country: activity data")
        joinDF
    }
    
    override def collectAccRegisterData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        var regPath = params(Constants.Parameters.ACC_REGISTER_PATH)
        var countryPath = params(Constants.Parameters.MAPPING_COUNTRY)
        
        var lstRegFiles = DataUtils.getListFiles(regPath, logDate, timing);
        var regDF = sqlContext.read.option("mergeSchema", "true").parquet(lstRegFiles: _*).select("game_code", calcId).distinct
        
        var lstMappingFiles = DataUtils.getListFiles(countryPath, logDate, timing);
        var countryDF = sqlContext.read.option("mergeSchema", "true").parquet(lstMappingFiles: _*).sort("log_date").dropDuplicates(Seq(calcId)).select(calcId,"country_code")
        
        var joinDF = regDF.as('r).join(countryDF.as('c), 
                regDF(calcId) === countryDF(calcId), "left_outer")
                .select("r.game_code","r." + calcId, "c.country_code")
        //joinDF.show
        Common.logger("Country: accregister data")
        joinDF
    }
    
    override def collectPaymentData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        var paymentPath = params(Constants.Parameters.PAYMENT_PATH)
        var countryPath = params(Constants.Parameters.MAPPING_COUNTRY)
        
        var lstPaymentFiles = DataUtils.getListFiles(paymentPath, logDate, timing);
        var paymentDF = sqlContext.read.option("mergeSchema", "true").parquet(lstPaymentFiles: _*)
        
        var lstMappingFiles = DataUtils.getListFiles(countryPath, logDate, timing);
        var countryDF = sqlContext.read.option("mergeSchema", "true").parquet(lstMappingFiles: _*).sort("log_date").dropDuplicates(Seq(calcId)).select(calcId,"country_code")
        
        var joinDF = paymentDF.as('p).join(countryDF.as('c), 
                paymentDF(calcId) === countryDF(calcId), "left_outer")
                .select("p.game_code", "p.log_date","p." + calcId, "p.net_amt", "p.gross_amt", "c.country_code")
        //joinDF.show
        Common.logger("Country: payment data")
        joinDF
    }
    
    override def collectFirstChargeData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {

        var paymentPath = params(Constants.Parameters.PAYMENT_PATH)
        var firstChargePath = params(Constants.Parameters.FIRSTCHARGE_PATH)
        var countryPath = params(Constants.Parameters.MAPPING_COUNTRY)
        
        var lstPaymentFiles = DataUtils.getListFiles(paymentPath, logDate, timing);
        var paymentDF = sqlContext.read.option("mergeSchema", "true").parquet(lstPaymentFiles: _*)
        
        var lstFirstChargeFiles = DataUtils.getListFiles(firstChargePath, logDate, timing);
        var firstDF = sqlContext.read.option("mergeSchema", "true").parquet(lstFirstChargeFiles: _*)
        
        var lstMappingFiles = DataUtils.getListFiles(countryPath, logDate, timing);
        var countryDF = sqlContext.read.option("mergeSchema", "true").parquet(lstMappingFiles: _*).sort("log_date").dropDuplicates(Seq(calcId)).select(calcId, "country_code")
        
        var joinCountryDF = firstDF.as('f).join(countryDF.as('c), 
                firstDF(calcId) === countryDF(calcId), "left_outer")
                .select("f.game_code", "f.log_date","f." + calcId, "c.country_code")
        
        var joinDF = joinCountryDF.as('c).join(paymentDF.as('p), 
                joinCountryDF(calcId) === paymentDF(calcId), "left_outer")
                .select("p.game_code", "p.log_date","c." + calcId, "p.net_amt", "p.gross_amt", "c.country_code")
        //joinDF.show
        Common.logger("Country: firstcharge data")
        joinDF
    }
    
    def collectNewUserRevenueData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        var paymentPath = params(Constants.Parameters.PAYMENT_PATH)
        var regPath = params(Constants.Parameters.ACC_REGISTER_PATH)
        var countryPath = params(Constants.Parameters.MAPPING_COUNTRY)
        
        var lstRegFiles = DataUtils.getListFiles(regPath, logDate, timing);
        var regDF = sqlContext.read.option("mergeSchema", "true").parquet(lstRegFiles: _*).select("game_code", calcId).distinct
        
        var lstPaymentFiles = DataUtils.getListFiles(paymentPath, logDate, timing);
        var paymentDF = sqlContext.read.option("mergeSchema", "true").parquet(lstPaymentFiles: _*)
        
        var lstMappingFiles = DataUtils.getListFiles(countryPath, logDate, timing);
        var countryDF = sqlContext.read.option("mergeSchema", "true").parquet(lstMappingFiles: _*).sort("log_date").dropDuplicates(Seq(calcId)).select(calcId,"country_code")
        
        var countryJoinDF = paymentDF.as('p).join(countryDF.as('c), 
                paymentDF(calcId) === countryDF(calcId), "left_outer")
                .select("p.game_code", "p.log_date","p." + calcId, "p.net_amt", "p.gross_amt", "c.country_code")
                
        var joinDF = countryJoinDF.as('cp).join(regDF.as('r), 
                countryJoinDF(calcId) === regDF(calcId), "leftsemi")
                .select("cp.game_code", "cp.log_date","cp." + calcId, "cp.net_amt", "p.gross_amt", "cp.country_code")
        //joinDF.show
        
        Common.logger("Country: new user revenue data")
        joinDF
    }
    
    def collectNewUserRetentionData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        var activityPath = params(Constants.Parameters.ACTIVITY_PATH)
        var regPath = params(Constants.Parameters.ACC_REGISTER_PATH)
        var countryPath = params(Constants.Parameters.MAPPING_COUNTRY)
        
        var regFile = DataUtils.getFile(regPath, logDate, timing);
        
        if (!DataUtils.isEmpty(regFile)) {
            var regDF = sqlContext.read.option("mergeSchema", "true").parquet(regFile).select(calcId)
            
            var activityFile = DataUtils.getFile(activityPath, logDate);
            var activityDF = sqlContext.read.option("mergeSchema", "true").parquet(activityFile).select(calcId).distinct
            
            var currentMappingFile = DataUtils.getFile(countryPath, logDate);
            var currentCountryDF = sqlContext.read.option("mergeSchema", "true").parquet(currentMappingFile).sort("log_date").dropDuplicates(Seq(calcId)).select(calcId,"country_code")
            
            var prevMappingFile = DataUtils.getFile(countryPath, logDate, timing);
            var prevCountryDF = sqlContext.read.option("mergeSchema", "true").parquet(prevMappingFile).sort("log_date").dropDuplicates(Seq(calcId)).select(calcId,"country_code")
            
            var countryActivityDF = activityDF.as('a).join(currentCountryDF.as('c), 
                    activityDF(calcId) === currentCountryDF(calcId), "left_outer").select("a." + calcId, "c.country_code")
            
            var countryRegDF = regDF.as('r).join(prevCountryDF.as('c), 
                    regDF(calcId) === prevCountryDF(calcId), "left_outer").select("r." + calcId, "c.country_code")
                    
            var joinDF = countryRegDF.as('pr).join(countryActivityDF.as('ca), 
                    countryRegDF(calcId) === countryActivityDF(calcId) && countryRegDF(groupId) === countryActivityDF(groupId), "left_outer")
                    .selectExpr("pr." + calcId + " as pr" + calcId,  "ca." + calcId + " as ca" + calcId,"pr.country_code")
            //joinDF.show
            
            Common.logger("Country: new user retention data")
            joinDF
        } else {
            
            // if return null, excution will stop here
            Common.logger("Country: new user retention data - " + regFile + " not existed!")
            null
        }
    }
    
    def collectUserRetentionData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        var activityPath = params(Constants.Parameters.ACTIVITY_PATH)
        var countryPath = params(Constants.Parameters.MAPPING_COUNTRY)
        
        var prevActivityFile = DataUtils.getFile(activityPath, logDate, timing);
        
        if (!DataUtils.isEmpty(prevActivityFile)) {
            
            var prevActivityDF = sqlContext.read.option("mergeSchema", "true").parquet(prevActivityFile).select(calcId).distinct
            
            var activityFile = DataUtils.getFile(activityPath, logDate);
            var activityDF = sqlContext.read.option("mergeSchema", "true").parquet(activityFile).select(calcId).distinct
            
            var currentMappingFile = DataUtils.getFile(countryPath, logDate);
            var currentCountryDF = sqlContext.read.option("mergeSchema", "true").parquet(currentMappingFile).sort("log_date").dropDuplicates(Seq(calcId)).select(calcId,"country_code")
            
            var prevMappingFile = DataUtils.getFile(countryPath, logDate, timing);
            var prevCountryDF = sqlContext.read.option("mergeSchema", "true").parquet(prevMappingFile).sort("log_date").dropDuplicates(Seq(calcId)).select(calcId,"country_code")
            
            var countryActivityDF = activityDF.as('ca).join(currentCountryDF.as('c), 
                    activityDF(calcId) === currentCountryDF(calcId), "left_outer")
                    .select("ca." + calcId, "c.country_code")
            
            var prevCountryActivityDF = prevActivityDF.as('pa).join(prevCountryDF.as('c), 
                    prevActivityDF(calcId) === prevCountryDF(calcId), "left_outer")
                    .select("pa." + calcId, "c.country_code")
                    
            var joinDF = prevCountryActivityDF.as('pa).join(countryActivityDF.as('ca), 
                    prevCountryActivityDF(calcId) === countryActivityDF(calcId) && prevCountryActivityDF(groupId) === countryActivityDF(groupId), "left_outer")
                    .selectExpr("pa." + calcId + " as pa" + calcId,  "ca." + calcId + " as ca" + calcId,"pa.country_code")
            //joinDF.show
            
            Common.logger("Country: user retention data")
            joinDF
        } else {
            
            // if return null, excution will stop here 
            Common.logger("Country: user retention data - " + prevActivityFile + " not existed!")
            null
        }
    }
    
    def collectFirstChargeRetentionData(sqlContext: SQLContext, params: Map[String, String]): DataFrame = {
        
        var payPath = params(Constants.Parameters.PAYMENT_PATH)
        var firstPayPath = params(Constants.Parameters.FIRSTCHARGE_PATH)
        var countryPath = params(Constants.Parameters.MAPPING_COUNTRY)
        
        var firstFile = DataUtils.getFile(firstPayPath, logDate, timing);
        
        if (!DataUtils.isEmpty(firstFile)) {
            
            var firstDF = sqlContext.read.option("mergeSchema", "true").parquet(firstFile).select(calcId)
            
            var payFile = DataUtils.getFile(payPath, logDate);
            var payDF = sqlContext.read.option("mergeSchema", "true").parquet(payFile).select(calcId).distinct
            
            var currentMappingFile = DataUtils.getFile(countryPath, logDate);
            var currentCountryDF = sqlContext.read.option("mergeSchema", "true").parquet(currentMappingFile).sort("log_date").dropDuplicates(Seq(calcId)).select(calcId,"country_code")
            
            var prevMappingFile = DataUtils.getFile(countryPath, logDate, timing);
            var prevCountryDF = sqlContext.read.option("mergeSchema", "true").parquet(prevMappingFile).sort("log_date").dropDuplicates(Seq(calcId)).select(calcId,"country_code")
            
            var countryPayDF = payDF.as('p).join(currentCountryDF.as('c), 
                    payDF(calcId) === currentCountryDF(calcId), "left_outer")
                    .select("p." + calcId, "c.country_code")
            
            var countryFirstDF = firstDF.as('f).join(prevCountryDF.as('c), 
                    firstDF(calcId) === prevCountryDF(calcId), "left_outer")
                    .select("f." + calcId, "c.country_code")
                    
            var joinDF = countryFirstDF.as('pp).join(countryPayDF.as('cp), 
                    countryFirstDF(calcId) === countryPayDF(calcId) && countryFirstDF(groupId) === countryPayDF(groupId), "left_outer")
                    .selectExpr("pp." + calcId + " as pp" + calcId,  "cp." + calcId + " as cp" + calcId,"pp.country_code")
            //joinDF.show
            
            Common.logger("Country: firstcharge retention data")
            joinDF
        } else {
            
            // if return null, excution will stop here
            Common.logger("Country: firstcharge retention data - " + firstFile + " not existed!")
            null
        }
    }
}