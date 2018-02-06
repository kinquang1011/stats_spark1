package vng.stats.ub.normalizer.hcatalog

import org.apache.spark.{SparkConf, SparkContext}
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2.{HiveQueryConfig, LoginLogoutFormatter, PaymentFormatter}
import vng.stats.ub.utils.Constants
import scala.util.matching.Regex
object QMobileFormatter extends IngameFormatter("3qmobile", "3qmobile") {

    def main(args: Array[String]) {
        
        initParameters(args)

        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }

    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        var channelRegex = new Regex("^(?:(?![i])[a-z])*")
        
         val getChannel = (params: Seq[Any]) =>{
            val channel = channelRegex.findFirstIn(params(0).toString().toLowerCase())
            channel.getOrElse("")
         }
            
        val sf = Constants.LOGIN_LOGOUT_FIELD
        var partition = getPartition(logDate)
        var loginQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("'login'", sf.ACTION)
            select("user_id", sf.ID)
            select("server_id", sf.SID)
            select("char_id", sf.RID)
            select("getChannel(array(char_id))", sf.CHANNEL)
            select("pve_level", sf.LEVEL)
            select("device_iemi", sf.DID)
            select("'0'", sf.ONLINE_TIME)
            select("device_type", sf.DEVICE)
            select("system_type", sf.OS)
            select("system_version", sf.OS_VERSION)
            from("3qm.login")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
            registUdf("getChannel", getChannel)
        }

        var logoutQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("'logout'", sf.ACTION)
            select("user_id", sf.ID)
            select("server_id", sf.SID)
            select("char_id", sf.RID)
            select("getChannel(array(char_id))", sf.CHANNEL)
            select("pve_level", sf.LEVEL)
            select("''", sf.DID)
            select("online", sf.ONLINE_TIME)
            select("''", sf.DEVICE)
            select("''", sf.OS)
            select("''", sf.OS_VERSION)
            from("3qm.logout")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
            registUdf("getChannel", getChannel)
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
        var channelRegex = new Regex("^(?:(?![i])[a-z])*")
       val getChannel = (params: Seq[Any]) =>{
            val channel = channelRegex.findFirstIn(params(0).toString().toLowerCase())
            channel.getOrElse("")
         }
        var partition = getPartition(logDate)
        
        var rechargeQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("user_id", sf.ID)
            select("server_id", sf.SID)
            select("char_id", sf.RID)
            select("getChannel(array(char_id))", sf.CHANNEL)
            select("gold2", sf.GROSS_AMT)
            select("gold2", sf.NET_AMT)
            select("pve_level", sf.LEVEL)
            from("3qm.recharge")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
            registUdf("getChannel", getChannel)
        }
        var rechargeConfig = new FormatterConfig(null, null, null, null)
        rechargeConfig.hiveQueryConfig = rechargeQuery
        rechargeConfig.inputFileType = Constants.INPUT_FILE_TYPE.HIVE

        var activityPath = getOutputPath(logDate, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER)
        
        var on: Array[String] = Array(sf.ID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sf.ID -> "is not null"))
        var fields: Map[String, Array[String]] = Map("A" -> Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.RID, sf.SID, sf.GROSS_AMT, sf.NET_AMT, sf.CHANNEL, sf.LEVEL),
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

