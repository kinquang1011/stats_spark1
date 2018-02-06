package vng.stats.ub.normalizer.hcatalog

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2.CcuFormatter
import vng.stats.ub.normalizer.format.v2.HiveQueryConfig
import vng.stats.ub.normalizer.format.v2.LoginLogoutFormatter
import vng.stats.ub.normalizer.format.v2.PaymentFormatter
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DateTimeUtils

object FarmerySeaFormatter extends IngameFormatter("sfmigsn", "sfmigsn") {

    def main(args: Array[String]) {
        
        initParameters(args)

        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        if(!mapParameters.contains("hourly")){
            
            //addFunction(Constants.NEW_LOGIN_DEVICE_TYPE, true)
            //addFunction(Constants.NEW_PAID_DEVICE_TYPE, true)
        }
        run(sc)
        sc.stop()
    }

    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
        val sf = Constants.CCU
        var partition = getPartition(logDate)
        val formatDate = (params: Seq[Any]) => {
            val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            var logDate = params(0).toString
            
            var min = logDate.substring(14,16).toInt
            min = min - (min % 5)
            
            var newDate = DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH", logDate) + ":%02d:00".format(min)
            newDate
        }
        
        var ccuQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("formatDate(array(log_date))", sf.LOG_DATE)
            select("server_id", sf.SID)
            select("ccu", sf.CCU)

            from(gameCode + ".ccu")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
            registUdf("formatCcu", formatDate)
        }
        var ccuConfig = new FormatterConfig(null, null, null, null)
        ccuConfig.hiveQueryConfig = ccuQuery
        ccuConfig.inputFileType = Constants.INPUT_FILE_TYPE.HIVE

        var config: Array[FormatterConfig] = Array(
            ccuConfig
        )
        var formatter = new CcuFormatter(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.CCU_OUTPUT_FOLDER))
        formatter.format(sc)
    }
    
    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        val sf = Constants.LOGIN_LOGOUT_FIELD
        var partition = getPartition(logDate)
        var loginQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("'login'", sf.ACTION)
            select("account_name", sf.ID)
            select("role_id", sf.RID)
            select("server_id", sf.SID)
            select("level", sf.LEVEL)
            select("ip", sf.IP)
            select("'0'", sf.ONLINE_TIME)
            from(gameCode + ".login")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
        }
        var logoutQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("logout_date", sf.LOG_DATE)
            select("'logout'", sf.ACTION)
            select("account_name", sf.ID)
            select("role_id", sf.RID)
            select("server_id", sf.SID)
            select("level", sf.LEVEL)
            select("ip", sf.IP)
            select("online_time", sf.ONLINE_TIME)
            from(gameCode + ".logout")
            where("date_format(logout_date, \"yyyy-MM-dd\")", logDate)
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
        var partition = getPartition(logDate)
        var rechargeQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("user_id", sf.ID)
            select("server_id", sf.SID)
            select("ip_addr", sf.IP)
            select("trans_id", sf.TRANS_ID)
            select("zing_xu", sf.GROSS_AMT)
            select("zing_xu", sf.NET_AMT)
            select("role_level", sf.LEVEL)
            from(gameCode + ".recharge")
            where("error_id", "1")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
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
}

