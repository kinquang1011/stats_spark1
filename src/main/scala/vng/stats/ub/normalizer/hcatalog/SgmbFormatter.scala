package vng.stats.ub.normalizer.hcatalog

import org.apache.spark.{SparkConf, SparkContext}
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{Common, Constants}


import org.apache.spark.sql.functions.udf


object SgmbFormatter extends IngameFormatter ("cgmfbs", "sgmb") {

    def main(args: Array[String]) {
        initParameters(args)
        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {}
    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        val sf = Constants.LOGIN_LOGOUT_FIELD
        var partition = getPartition(logDate)

        val osTransform = (params: Seq[Any]) => {
            var osRaw = params(0).toString
            var rs:String = "windows"
            if (osRaw.toLowerCase.startsWith("android")) {
                rs = "android"
            } else if (osRaw.toLowerCase.startsWith("iphone")) {
                rs = "ios"
            }
            rs
        }

        var loginQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("'login'", sf.ACTION)
            select("account_name", sf.ID)
            select("role_id", sf.RID)
            select("server_id", sf.SID)
            select("level", sf.LEVEL)
            select("login_source", sf.DEVICE)
            select("column18", sf.PACKAGE_NAME)
            //select("osTransform(column15)", sf.OS)
            select("osTransform(array(column15,column14))", sf.OS)
            select("'0'", sf.ONLINE_TIME)
            from(gameFolder + ".login")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
            registUdf("osTransform", osTransform)
        }
        var logoutQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("'logout'", sf.ACTION)
            select("account_name", sf.ID)
            select("role_id", sf.RID)
            select("server_id", sf.SID)
            select("level", sf.LEVEL)
            select("''", sf.DEVICE)
            select("''", sf.PACKAGE_NAME)
            select("''", sf.OS)
            select("online_time", sf.ONLINE_TIME)
            from(gameFolder + ".logout")
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
            select("account_name", sf.ID)
            select("server_id", sf.SID)
            select("gross_rev", sf.GROSS_AMT)
            select("gross_rev", sf.NET_AMT)
            select("action_id", sf.PAY_CHANNEL)
            from(gameFolder + ".recharge")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            //where("ds", "20161104")
            where_in("ds", partition)
        }
        var rechargeConfig = new FormatterConfig(null, null, null, null)
        rechargeConfig.hiveQueryConfig = rechargeQuery
        rechargeConfig.inputFileType = Constants.INPUT_FILE_TYPE.HIVE

        //join
        var activityPath = getOutputPath(logDate, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER)
        var on: Array[String] = Array(sf.ID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sf.ID -> "is not null"))
        var fields: Map[String, Array[String]] = Map("A" -> Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.GROSS_AMT, sf.NET_AMT, sf.PAY_CHANNEL),
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

