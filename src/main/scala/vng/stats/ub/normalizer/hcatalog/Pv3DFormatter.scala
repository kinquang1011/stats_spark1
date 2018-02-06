package vng.stats.ub.normalizer.hcatalog

import org.apache.spark.{SparkConf, SparkContext}
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2.{HiveQueryConfig, LoginLogoutFormatter, PaymentFormatter}
import vng.stats.ub.utils.Constants
import scala.util.matching.Regex

object Pv3DFormatter extends IngameFormatter("pv3d", "pv3d") {

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
            select("user_id", sf.ID)
            select("server_id", sf.SID)
            select("channel", sf.CHANNEL)
            select("level", sf.LEVEL)
            select("role_id", sf.RID)
            select("device_id", sf.DID)
            select("ip", sf.IP)
            select("'0'", sf.ONLINE_TIME)
            from("pv3d.login")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
        }

        var logoutQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("'logout'", sf.ACTION)
            select("user_id", sf.ID)
            select("server_id", sf.SID)
            select("channel", sf.CHANNEL)
            select("level", sf.LEVEL)
            select("role_id", sf.RID)
            select("device_id", sf.DID)
            select("''", sf.IP)
            select("online_time", sf.ONLINE_TIME)
            from("pv3d.logout")
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
        
        var partition = getPartition(logDate)
        
        var rechargeQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("user_id", sf.ID)
            select("server_id", sf.SID)
            select("role_id", sf.RID)
            select("level", sf.LEVEL)
            select("channel", sf.CHANNEL)
            select("recharge_channel", sf.PAY_CHANNEL)
            select("money*200", sf.GROSS_AMT)
            select("money*200", sf.NET_AMT)
            select("ip", sf.IP)
            from("pv3d.recharge")
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
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
      
    }
}

