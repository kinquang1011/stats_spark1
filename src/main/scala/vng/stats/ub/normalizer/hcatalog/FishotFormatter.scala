package vng.stats.ub.normalizer.hcatalog

import org.apache.spark.{SparkConf, SparkContext}
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.Constants

object FishotFormatter extends IngameFormatter("fishot", "fishot") {

    def main(args: Array[String]) {
        
        initParameters(args)

        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        if(!mapParameters.contains("hourly")){
            
            addFunction(Constants.NEW_LOGIN_DEVICE_TYPE, true)
            addFunction(Constants.NEW_PAID_DEVICE_TYPE, true)
        }
        run(sc)
        sc.stop()
    }

    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
        val sf = Constants.CCU
        var partition = getPartition(logDate)
        var ccuQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("server_id", sf.SID)
            select("ccu", sf.CCU)
            from(gameCode + ".ccu")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
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
            select("user_id", sf.ID)
            select("server_id", sf.SID)
            select("role_id", sf.RID)
            select("level", sf.LEVEL)
            select("device_name", sf.DEVICE)
            select("os_platform", sf.OS)
            select("os_version", sf.OS_VERSION)
            select("device_id", sf.DID)
            select("download_source", sf.CHANNEL)
            select("'0'", sf.ONLINE_TIME)
            from(gameCode + ".login")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where("result", "1")
            where_in("ds", partition)
        }
        var logoutQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("'logout'", sf.ACTION)
            select("user_id", sf.ID)
            select("server_id", sf.SID)
            select("role_id", sf.RID)
            select("level", sf.LEVEL)
            select("device_name", sf.DEVICE)
            select("os_platform", sf.OS)
            select("os_version", sf.OS_VERSION)
            select("device_id", sf.DID)
            select("''", sf.CHANNEL)
            select("total_online_second", sf.ONLINE_TIME)
            from(gameCode + ".logout")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where("result", "1")
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
            select("gross_revenue", sf.GROSS_AMT)
            select("net_revenue", sf.NET_AMT)
            select("transaction_id", sf.TRANS_ID)
            select("ip_paying", sf.IP)
            select("level", sf.LEVEL)
            select("payment_gateway", sf.PAY_CHANNEL)
            select("device_id", sf.DID)
            from(gameCode + ".recharge")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where("result", "1")
            where_in("ds", partition)
        }
        var rechargeConfig = new FormatterConfig(null, null, null, null)
        rechargeConfig.hiveQueryConfig = rechargeQuery
        rechargeConfig.inputFileType = Constants.INPUT_FILE_TYPE.HIVE

        //join
        var activityPath = getOutputPath(logDate, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER)
        var on: Array[String] = Array(sf.ID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sf.ID -> "is not null"))
        var fields: Map[String, Array[String]] = Map("A" -> Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.GROSS_AMT, sf.NET_AMT, sf.TRANS_ID, sf.IP, sf.LEVEL, sf.PAY_CHANNEL, sf.DID),
            "B" -> Array(sf.DEVICE, sf.OS, sf.OS_VERSION))
        var formatOS: FormatterConfig = new FormatterConfig(null, null, Array(sf.DEVICE, sf.OS, sf.OS_VERSION), activityPath, Map("type" -> "left join", "fields" -> fields, "where" -> where, "on" -> on))
        formatOS.setInputFileType("parquet")
        formatOS.setFieldDistinct(Array("id"))

        var config: Array[FormatterConfig] = Array(
            rechargeConfig,
            formatOS
        )
        
        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.PAYMENT_OUTPUT_FOLDER))
        formatter.format(sc)
    }
}

