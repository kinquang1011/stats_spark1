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
import vng.stats.ub.utils.Common

object SkyGardenGlobalFormatter extends IngameFormatter("cgmbgfbs1", "cgmbgfbs1") {

    def main(args: Array[String]) {
        
        initParameters(args)

        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }
    
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
        val sf = Constants.CCU
        var partition = getPartition(logDate, numOfHourly = 2)
        var rechargeQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("from_utc_timestamp(log_date, 'Asia/Ho_Chi_Minh')", sf.LOG_DATE)
            select("server_id", sf.SID)
            select("ccu", sf.CCU)

            from(gameCode + ".ccu")
            where("date_format(from_utc_timestamp(log_date, 'Asia/Ho_Chi_Minh'), \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
        }
        var ccuConfig = new FormatterConfig(null, null, null, null)
        ccuConfig.hiveQueryConfig = rechargeQuery
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
        var partition = getPartition(logDate, numOfHourly = 2)
        var loginQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("from_utc_timestamp(log_date, 'Asia/Ho_Chi_Minh')", sf.LOG_DATE)
            select("'login'", sf.ACTION)
            select("account_name", sf.ID)
            select("role_id", sf.RID)
            select("server_id", sf.SID)
            select("level", sf.LEVEL)
            select("ip", sf.IP)
            select("'0'", sf.ONLINE_TIME)
            select("login_source", sf.DEVICE)
            from(gameCode + ".login")
            where("date_format(from_utc_timestamp(log_date, 'Asia/Ho_Chi_Minh'), \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
        }
        var logoutQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("from_utc_timestamp(log_date, 'Asia/Ho_Chi_Minh')", sf.LOG_DATE)
            select("'logout'", sf.ACTION)
            select("account_name", sf.ID)
            select("role_id", sf.RID)
            select("server_id", sf.SID)
            select("level", sf.LEVEL)
            select("ip", sf.IP)
            select("online_time", sf.ONLINE_TIME)
            select("''", sf.DEVICE)
            from(gameCode + ".logout")
            where("date_format(from_utc_timestamp(log_date, 'Asia/Ho_Chi_Minh'), \"yyyy-MM-dd\")", logDate)
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
        var partition = getPartition(logDate, numOfHourly = 2)
        var rechargeQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("from_utc_timestamp(log_date, 'Asia/Ho_Chi_Minh')", sf.LOG_DATE)
            select("account_name", sf.ID)
            select("role_id", sf.RID)
            select("server_id", sf.SID)
            select("pay_gateway", sf.PAY_CHANNEL)
            select("gross_rev_user", sf.GROSS_AMT)
            select("gross_rev", sf.NET_AMT)
            select("level", sf.LEVEL)
            select("transaction_id", sf.TRANS_ID)
            select("ip", sf.IP)
            from(gameCode + ".recharge")
            where("date_format(from_utc_timestamp(log_date, 'Asia/Ho_Chi_Minh'), \"yyyy-MM-dd\")", logDate)
            where("result", "0")
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

