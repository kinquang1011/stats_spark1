package vng.stats.ub.normalizer.hcatalog

import org.apache.spark.{SparkConf, SparkContext}
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2.{HiveQueryConfig, LoginLogoutFormatter, PaymentFormatter}
import vng.stats.ub.utils.Constants
import scala.util.matching.Regex
object TlbbmFormatter extends IngameFormatter("tlbbm", "tlbbm") {

    def main(args: Array[String]) {
        
        initParameters(args)

        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }

    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        val sf = Constants.LOGIN_LOGOUT_FIELD
        var partition = getPartition(logDate)
        var loginQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("'login'", sf.ACTION)
            select("userid", sf.ID)
            select("serverid", sf.SID)
            select("roleid", sf.RID)
            select("gamechannel", sf.CHANNEL)
            select("level", sf.LEVEL)
            select("deviceid", sf.DID)
            select("ip", sf.IP)
            from("tlbbm.login")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
        }

        var logoutQuery = new HiveQueryConfig {
             select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("'logout'", sf.ACTION)
            select("userid", sf.ID)
            select("serverid", sf.SID)
            select("roleid", sf.RID)
            select("gamechannel", sf.CHANNEL)
            select("level", sf.LEVEL)
            select("deviceid", sf.DID)
            select("''", sf.IP)
            from("tlbbm.logout")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
        }

        var loginConfig = new FormatterConfig(null, null, null, null)
        loginConfig.hiveQueryConfig = loginQuery
        loginConfig.inputFileType = Constants.INPUT_FILE_TYPE.HIVE

        var logoutConfig = new FormatterConfig(null, null, null, null)
        logoutConfig.hiveQueryConfig = logoutQuery
        logoutConfig.inputFileType = Constants.INPUT_FILE_TYPE.HIVE

        var config: Array[FormatterConfig] = Array(
            loginConfig,
            logoutConfig
        )
        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER))
        formatter.format(sc)

    }

    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        val sf = Constants.PAYMENT_FIELD
        val getAmt = (value: Seq[Any]) =>{
            
            var amt = value(0).toString().toLong*200
            if(value(0).toString() == "3000" && value(1).toString() < "50"){
                amt = 500000L
            }
            amt.toString()
         }
        
        var partition = getPartition(logDate)
        
        var rechargeQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("userid", sf.ID)
            select("serverid", sf.SID)
            select("roleid", sf.RID)
            select("gamechannel", sf.CHANNEL)
            select("rechargechannel", sf.PAY_CHANNEL)
            select("getAmt(array(valuequantity,level))", sf.GROSS_AMT)
            select("getAmt(array(valuequantity,level))", sf.NET_AMT)
            select("level", sf.LEVEL)
            select("ip", sf.IP)
            from("tlbbm.recharge")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
            registUdf("getAmt", getAmt)
        }
        var rechargeConfig = new FormatterConfig(null, null, null, null)
        rechargeConfig.hiveQueryConfig = rechargeQuery
        rechargeConfig.inputFileType = Constants.INPUT_FILE_TYPE.HIVE
        
        
        var config: Array[FormatterConfig] = Array(
            rechargeConfig
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.PAYMENT_OUTPUT_FOLDER))
        formatter.format(sc)

    }
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
      
    }
}

