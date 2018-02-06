package vng.stats.ub.report2.jobsubmit

import scala.util.Try
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import vng.stats.ub.report2.group.KpiAccountRegister
import vng.stats.ub.report2.group.KpiActiveUser
import vng.stats.ub.report2.group.KpiCcu
import vng.stats.ub.report2.group.KpiFirstChargeRetention
import vng.stats.ub.report2.group.KpiNewUserPaying
import vng.stats.ub.report2.group.KpiNewUserRetention
import vng.stats.ub.report2.group.KpiPaying
import vng.stats.ub.report2.group.KpiPayingFirstCharge
import vng.stats.ub.report2.group.KpiRoleRegister
import vng.stats.ub.report2.group.KpiServerAccountRegister
import vng.stats.ub.report2.group.KpiUserRetention
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.utils.DateTimeUtils

object Rerun {
    
    def main(args: Array[String]) {
        
        var parameters = DataUtils.getParameters(args);
        var runType =  parameters(Constants.Parameters.RUN_TYPE)
        
        if("run" == runType){
            
            Common.logger("RUN...")
            
            var logDate = parameters(Constants.Parameters.LOG_DATE)
            parameters = parameters + (Constants.Parameters.FROM_DATE -> logDate)
            parameters = parameters + (Constants.Parameters.TO_DATE -> logDate)
            
            rerun(parameters)
        } else if("rerun" == runType){
            
            Common.logger("RERUN...")
            rerun(parameters)
        }
    }
    
    def rerun(parameters: Map[String, String]) {
        
        var jobName = parameters(Constants.Parameters.JOB_NAME)
        val conf = new SparkConf().setAppName(jobName)
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        
        var sc = new SparkContext(conf)
        
        var newParams = parameters
        
        var runType =  newParams(Constants.Parameters.RUN_TYPE)
        var gameCode = newParams(Constants.Parameters.GAME_CODE)
        var timings = newParams(Constants.Parameters.RUN_TIMING)
        var fromDate = newParams(Constants.Parameters.FROM_DATE)
        var toDate = newParams(Constants.Parameters.TO_DATE)
        
        var openDate = "2016-01-01"
        
        if(GameInfo.GAMES.contains(gameCode)){
            
            openDate = GameInfo.GAMES(gameCode)
        }
        
        var lstRerunDate = DateTimeUtils.getListDate(fromDate, toDate);
        
        var lstTiming = List[String]()
        timings.split(",").foreach { value => lstTiming = lstTiming ::: List(value)}
        
        var reportNumber = newParams(Constants.Parameters.REPORT_NUMBER)
        var groupId: String = ""
        
        var isExist = Try {
            groupId = parameters(Constants.Parameters.GROUP_ID)
        }
        
        lstTiming.foreach {
            timing =>
                {
                    lstRerunDate.foreach {
                        date =>
                            {
                                breakable {
                                    
                                    if(runType == "rerun"){
                                        
                                        if((timing == Constants.Timing.AC30 || timing == Constants.Timing.AC60) && !DateTimeUtils.isEndOfMonth(date)){
                                            Common.logger("Date is not end of month!")
                                            break
                                        }
                                        
                                        if(timing == Constants.Timing.AC7 && !DateTimeUtils.isEndOfWeek(date)){
                                            Common.logger("Date is not end of week!")
                                            break
                                        }
                                    }
                                    
                                    if(openDate.compareTo(date) > 0){
                                        
                                        Common.logger("Run date (" + date + ") is not valid, open date = " + openDate)
                                        break
                                    }
                                    
                                    Common.logger("Run: " + date)
                                    Common.logger("Report Number: " + reportNumber)
                                    Common.logger("Timing: " + timing)
                                    
                                    newParams = newParams + (Constants.Parameters.LOG_DATE -> date)
                                    newParams = newParams + (Constants.Parameters.TIMING -> timing)
    
                                    if(groupId == ""){
                                        runGameReport(reportNumber, newParams, sc);
                                    } else {
                                        runGroupReport(reportNumber, newParams, sc);
                                    }
                                    
                                    Common.logger("----------------------------------------------------------------")
                                }
                            }
                    }
                }
        }
        
        sc.stop()
    }
    
    def runGameReport(reportNumber: String, params: Map[String, String], sc: SparkContext) :Unit = {
        
        var isSuccess = Try()
        
        reportNumber.split("-").foreach { number => 
                           
            Common.logger("Number: " + number)
            number match {
                
                case Constants.ReportNumber.CCU => {
                    
                    isSuccess = Try {
                        vng.stats.ub.report2.game.KpiCcu.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GAME CCU FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.ACCOUNT_REGISTER => {
                    
                    isSuccess = Try {
                        vng.stats.ub.report2.game.KpiAccountRegister.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GAME ACCOUNT REGISTER FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.ACTIVE_USER => {
                    
                    isSuccess = Try {
                        vng.stats.ub.report2.game.KpiActiveUser.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GAME ACTIVE USER FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.USER_RETENTION => {
                    
                    isSuccess = Try {
                        vng.stats.ub.report2.game.KpiUserRetention.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GAME USER RETENTION FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.NEWUSER_RETENTION => {
                    
                    isSuccess = Try {
                        vng.stats.ub.report2.game.KpiNewUserRetention.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GAME NEW USER RETENTION FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.NEWUSER_REVENUE => {
                    
                    isSuccess = Try {
                        vng.stats.ub.report2.game.KpiNewUserPaying.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GAME NEW USER REVENUE FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.REVENUE => {
                    
                    isSuccess = Try {
                        vng.stats.ub.report2.game.KpiPaying.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GAME REVENUE FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.FIRST_CHARGE => {
                    
                    isSuccess = Try {
                        vng.stats.ub.report2.game.KpiPayingFirstCharge.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GAME FIRST CHARGE FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.FIRST_CHARGE_RETENTION => {
                    
                    isSuccess = Try {
                        vng.stats.ub.report2.game.KpiFirstChargeRetention.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GAME FIRST CHARGE RETENTION FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.PLAYING_TIME => {
                    
                    isSuccess = Try {
                        vng.stats.ub.report2.game.GameUserPlayingTime.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GAME USER PLAYING TIME FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.GAME_RETENTION => {
                    
                    isSuccess = Try {
                        vng.stats.ub.report2.game.GameRetention.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GAME RETENTION FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case _ => {
                    Common.logger("Game Report: Number Not Found ")
                }
            }
        }
    }
    
    def runGroupReport(reportNumber: String, params: Map[String, String], sc: SparkContext) :Unit = {
        
        var isSuccess = Try()
        
        reportNumber.split("-").foreach { number => 
                                            
            number match {

                case Constants.ReportNumber.CCU => {
                    
                    isSuccess = Try {
                        KpiCcu.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GROUP CCU FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.ACCOUNT_REGISTER => {
                    
                    isSuccess = Try {
                        KpiAccountRegister.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GROUP ACCOUNT REGISTER FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.ACTIVE_USER => {
                    
                    isSuccess = Try {
                        KpiActiveUser.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GROUP ACTIVE USER FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.USER_RETENTION => {
                    
                    isSuccess = Try {
                        KpiUserRetention.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GROUP USER RETENTION FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.NEWUSER_RETENTION => {
                    
                    isSuccess = Try {
                        KpiNewUserRetention.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GROUP NEW USER RETENTION FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.NEWUSER_REVENUE => {
                    
                    isSuccess = Try {
                        KpiNewUserPaying.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GROUP NEW USER REVENUE FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.REVENUE => {
                    
                    isSuccess = Try {
                        KpiPaying.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GROUP REVENUE FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.FIRST_CHARGE => {
                    
                    isSuccess = Try {
                        KpiPayingFirstCharge.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GROUP FIRST CHARGE FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.FIRST_CHARGE_RETENTION => {
                    
                    isSuccess = Try {
                        KpiFirstChargeRetention.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GROUP FIRST CHARGE RETENTION FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.ROLE_REGISTER => {
                    
                    isSuccess = Try {
                        KpiRoleRegister.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GROUP ROLE REGISTER FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case Constants.ReportNumber.SERVER_ACCOUNT_REGISTER => {
                    
                    isSuccess = Try {
                        KpiServerAccountRegister.rerun(params, sc)
                    }
                    
                    if(isSuccess.isFailure){
                        Common.logger("GROUP SERVER ACCOUNT REGISTER FAIL: "+ isSuccess)
                    }
                    Common.logger("----------------------------------------------------------------")
                }
                case _ => {
                    Common.logger("Group Report: Number " + number + " not found ")
                }
            }
        }
    }
}