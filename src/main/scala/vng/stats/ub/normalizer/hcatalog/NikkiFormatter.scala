package vng.stats.ub.normalizer.hcatalog

import org.apache.spark.{SparkConf, SparkContext}
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2.{HiveQueryConfig, LoginLogoutFormatter, PaymentFormatter}
import vng.stats.ub.utils.Constants
import scala.util.matching.Regex

object NikkiFormatter extends IngameFormatter("nikki", "nikki") {

    def main(args: Array[String]) {
        
        initParameters(args)

        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }

    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        
          val getOs = (params: Seq[Any]) =>{
            var device = ""
            var os = "ios"
            if(params(0).toString() == "1"){
                os = "android"
            }
           os
         }
          val getDevice =(params: Seq[Any]) =>{
            var device = ""
            device = params(0).toString().split(",").apply(0)
            device
         }
        val sf = Constants.LOGIN_LOGOUT_FIELD
        var partition = getPartition(logDate)
        var loginQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("'login'", sf.ACTION)
            select("user_id", sf.ID)
            select("server_id", sf.SID)
            select("login_channel", sf.CHANNEL)
            select("level", sf.LEVEL)
            select("device_id", sf.DID)
            select("'0'", sf.ONLINE_TIME)
            select("getOs(array(plat_id))", sf.OS)
            select("getDevice(array(system_hardware))", sf.DEVICE)
            from("nikki.login")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
            registUdf("getOs", getOs)
            registUdf("getDevice", getDevice)
        }

        var logoutQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("'logout'", sf.ACTION)
            select("user_id", sf.ID)
            select("server_id", sf.SID)
            select("login_channel", sf.CHANNEL)
            select("level", sf.LEVEL)
            select("device_id", sf.DID)
            select("onlinetime", sf.ONLINE_TIME)
            select("getOs(array(plat_id))", sf.OS)
            select("getDevice(array(system_hardware))", sf.DEVICE)
            from("nikki.logout")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
            registUdf("getOs", getOs)
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
            select("channel", sf.CHANNEL)
            select("trans_id", sf.TRANS_ID)
            select("money", sf.GROSS_AMT)
            select("money", sf.NET_AMT)
            from("nikki.recharge")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
        }
        var rechargeConfig = new FormatterConfig(null, null, null, null)
        rechargeConfig.hiveQueryConfig = rechargeQuery
        rechargeConfig.inputFileType = Constants.INPUT_FILE_TYPE.HIVE

        var activityPath = getOutputPath(logDate, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER)
        
        var on: Array[String] = Array(sf.ID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sf.ID -> "is not null"))
        var fields: Map[String, Array[String]] = Map("A" -> Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.TRANS_ID, sf.GROSS_AMT, sf.NET_AMT, sf.CHANNEL),
                "B" -> Array(sf.DEVICE, sf.OS, sf.OS_VERSION))

        var formatOS: FormatterConfig = new FormatterConfig(null, null, Array(sf.DEVICE, sf.OS, sf.OS_VERSION), activityPath, Map("type" -> "left join", "fields" -> fields, "where" -> where, "on" -> on))
        formatOS.setInputFileType("parquet")
        formatOS.setFieldDistinct(Array("id"))
        
        
        var config: Array[FormatterConfig] = Array(
            rechargeConfig, formatOS
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.PAYMENT_OUTPUT_FOLDER))
        formatter.format(sc)

    }
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
      
    }
}

