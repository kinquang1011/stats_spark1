package vng.stats.ub.normalizer.hcatalog

import org.apache.spark.{SparkConf, SparkContext}
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.Constants

object CubeFarmFormatter extends IngameFormatter("cfgfbs1", "cfgfbs1") {

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
        var partition = getPartition(logDate, numOfHourly = 2)
        var loginQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("from_utc_timestamp(log_date, 'Asia/Ho_Chi_Minh')", sf.LOG_DATE)
            select("'login'", sf.ACTION)
            select("user_id", sf.ID)
            select("role_id", sf.RID)
            select("server_id", sf.SID)
            select("unknown19", sf.IP)
            select("level", sf.LEVEL)
            select("device_name", sf.DEVICE)
            select("device_id", sf.DID)
            select("os_platform", sf.OS)
            select("os_version", sf.OS_VERSION)
            select("'0'", sf.ONLINE_TIME)
            from(gameCode + ".login")
            where("result", "1")
            where("date_format(from_utc_timestamp(log_date, 'Asia/Ho_Chi_Minh'), \"yyyy-MM-dd\")", logDate)
            where("result", "0")
            where_in("ds", partition)
        }
        var logoutQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("from_utc_timestamp(log_date, 'Asia/Ho_Chi_Minh')", sf.LOG_DATE)
            select("'logout'", sf.ACTION)
            select("user_id", sf.ID)
            select("role_id", sf.RID)
            select("server_id", sf.SID)
            select("''", sf.IP)
            select("level", sf.LEVEL)
            select("device_name", sf.DEVICE)
            select("device_id", sf.DID)
            select("os_platform", sf.OS)
            select("os_version", sf.OS_VERSION)
            select("total_online_second", sf.ONLINE_TIME)
            from(gameCode + ".logout")
            where("result", "1")
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
            select("user_id", sf.ID)
            select("server_id", sf.SID)
            select("role_id", sf.RID)
            select("gross_revenue_user", sf.GROSS_AMT)
            select("gross_revenue", sf.NET_AMT)
            select("level", sf.LEVEL)
            select("transaction_id", sf.TRANS_ID)
            select("ip_paying", sf.IP)
            select("payment_gateway", sf.PAY_CHANNEL)
            from(gameCode + ".paying")
            where("result", "1")
            where("gross_revenue > 0")
            where("date_format(from_utc_timestamp(log_date, 'Asia/Ho_Chi_Minh'), \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
        }
        var rechargeConfig = new FormatterConfig(null, null, null, null)
        rechargeConfig.hiveQueryConfig = rechargeQuery
        rechargeConfig.inputFileType = Constants.INPUT_FILE_TYPE.HIVE

        //join
        var activityPath = getOutputPath(logDate, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER)
        var on: Array[String] = Array(sf.ID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sf.ID -> "is not null"))
        var fields: Map[String, Array[String]] = Map("A" -> Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.RID,sf.SID, sf.IP, sf.GROSS_AMT, sf.NET_AMT, sf.LEVEL, sf.TRANS_ID, sf.PAY_CHANNEL),
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
    
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
        val sf = Constants.CCU
        var partition = getPartition(logDate, numOfHourly = 2)
        var ccuQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("from_utc_timestamp(log_date, 'Asia/Ho_Chi_Minh')", sf.LOG_DATE)
            select("server_id", sf.SID)
            select("ccu", sf.CCU)
            from(gameCode + ".ccu")
            where("date_format(from_utc_timestamp(log_date, 'Asia/Ho_Chi_Minh'), \"yyyy-MM-dd\")", logDate)
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
}

