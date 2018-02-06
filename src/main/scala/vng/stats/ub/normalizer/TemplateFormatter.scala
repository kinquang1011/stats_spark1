package vng.stats.ub.normalizer

import vng.stats.ub.utils.Constants

object TemplateFormatter {
  

    def main(args: Array[String]) {
    
        var sF = Constants.PAYMENT_FIELD
        val PaymentSchema = Array(sF.GAME_CODE, sF.LOG_DATE, sF.ID, sF.SID, sF.RID, sF.LEVEL, sF.TRANS_ID, sF.CHANNEL, sF.GROSS_AMT, sF.NET_AMT, sF.XU_INSTOCK, sF.XU_SPENT, sF.XU_TOPUP, sF.IP)
    }
    
    def login(){
        
        var sF = Constants.LOGIN_LOGOUT_FIELD
        val LoginSchema = Array(sF.GAME_CODE, sF.ACTION, sF.CHANNEL, sF.LOG_DATE, sF.ID, sF.SID, sF.RID, sF.ONLINE_TIME, sF.LEVEL, sF.IP, sF.DID)
    }
    
    def roel(){
        
        var sF = Constants.ROLE_REGISTER_FIELD
        val LoginSchema = Array(sF.GAME_CODE, sF.LOG_DATE, sF.ID, sF.SID, sF.RID, sF.IP)
    }
    
}