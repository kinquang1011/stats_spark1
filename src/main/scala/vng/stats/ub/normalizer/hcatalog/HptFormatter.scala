package vng.stats.ub.normalizer.hcatalog

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DateTimeUtils
import org.apache.spark.sql.DataFrame

object HptFormatter extends IngameFormatter("hpt", "hpt") {

    def main(args: Array[String]) {
        initParameters(args)
        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        if (!mapParameters.contains("hourly")){
            addFunction(Constants.ROLE_REGISTER_TYPE, true)
            addFunction(Constants.CHARACTER_INFO_TYPE)
        }
        run(sc)
        sc.stop()
    }

    override def characterInfoFormatter(logDate: String, sc: SparkContext): Unit = {
        var formatter = new CharacterInfoFormatter(gameCode, logDate, null, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.CHARACTER_INFO_OUTPUT_FOLDER))
        formatter.format(sc)
    }

    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
        val sf = Constants.CCU
        var partition = getPartition(logDate, numOfDaily = 2)
        
        val extractServer = (params: Seq[Any]) => {
            var server = params(0).toString.toLowerCase
            server.takeRight(3).toInt.toString
        }
        
        val formatCcu = (params: Seq[Any]) => {
            val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            var logDate = params(0).toString
            
            var min = logDate.substring(14,16).toInt
            min = min - (min % 5)
            
            var newDate = DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH", logDate) + ":%02d:00".format(min)
            newDate
        }
        
        var rechargeQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("formatCcu(array(log_date))", sf.LOG_DATE)
            select("extractServer(array(server_id))", sf.SID)
            select("opr_id", sf.OS)
            select("online", sf.CCU)

            from("hpt.ccu")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
            registUdf("extractServer", extractServer)
            registUdf("formatCcu", formatCcu)
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
        var partition = getPartition(logDate, numOfDaily = 2)
        val extractServer = (params: Seq[Any]) => {
            var server = params(0).toString.toLowerCase
            server.takeRight(3).toInt.toString
        }
        
        val extractOs = (params: Seq[Any]) => {
            var model = params(0).toString.toLowerCase
            var rs:String = "windows"
            
            if (model.contains("android")) {
                rs = "android"
            } else if (model.contains("os")) {
                rs = "ios"
            }
            rs
        }
        
        val extractDevice = (params: Seq[Any]) => {
            var model = params(0).toString.toLowerCase
            var rs:String = ""
            
            if (model.contains("|")) {
                rs = model.split("|").apply(0)
            } else if (model.contains("*")) {
                rs = model.split("\\*").apply(0)
            }
            rs
        }
        
        var loginQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("login_date", sf.LOG_DATE)
            select("'login'", sf.ACTION)
            select("user_id", sf.ID)
            select("role_id", sf.RID)
            select("extractServer(array(server_id))", sf.SID)
            select("extractOs(array(model))", sf.OS)
            select("os_version", sf.OS_VERSION)
            select("extractDevice(array(model))", sf.DEVICE)
            from("hpt.login")
            where("date_format(login_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
            registUdf("extractOs", extractOs)
            registUdf("extractDevice", extractDevice)
            registUdf("extractServer", extractServer)
        }
        var logoutQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("logout_date", sf.LOG_DATE)
            select("'logout'", sf.ACTION)
            select("user_id", sf.ID)
            select("role_id", sf.RID)
            select("extractServer(array(server_id))", sf.SID)
            select("extractOs(array(model))", sf.OS)
            select("os_version", sf.OS_VERSION)
            select("extractDevice(array(model))", sf.DEVICE)
            from("hpt.logout")
            where("date_format(logout_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
            registUdf("extractOs", extractOs)
            registUdf("extractDevice", extractDevice)
            registUdf("extractServer", extractServer)
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
        var partition = getPartition(logDate, numOfDaily = 2)
        
        /*val calcGross = (params: Seq[Any]) => {
            
            var amt = 0.0
            amt = params(0) match {
                case 80 => 44775
                case 200 => 67275
                case 400 => 134775
                case 388 => 134775
                case 800 => 269775
                case 1200 => 449775
                case 2000 => 674775
                case 4000 => 1349775
                case 6750 => 2249775
            }
            
            amt.toString
        }
        
        val calcNet = (params: Seq[Any]) => {
            
            var amt = 0.0
            amt = params(0) match {
                case 80 => 33357.38
                case 200 => 50119.88
                case 400 => 100407.38
                case 388 => 100407.38
                case 800 => 200982.38
                case 1200 => 335082.38
                case 2000 => 502707.38
                case 4000 => 1005582.38
                case 6750 => 1676082.38
            }
            
            amt.toString
        }*/
        
        val extractServer = (params: Seq[Any]) => {
            var server = params(0).toString.toLowerCase
            server.takeRight(3).toInt.toString
        }
        
        val calcRev = (params: Seq[Any]) => {
            
            var amt = 0.0
            amt = params(0) match {
                case 80 => 20000
                case 200 => 50000
                case 400 => 100000
                case 388 => 100000
                case 800 => 200000
                case 1200 => 300000
                case 2000 => 500000
                case 4000 => 1000000
                case 6750 => 1680000
            }
            
            amt.toString
        }
        
        var rechargeQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("log_date", sf.LOG_DATE)
            select("user_id", sf.ID)
            select("role_id", sf.RID)
            select("extractServer(array(server_id))", sf.SID)
            select("calcRev(array(yuanbao))", sf.GROSS_AMT)
            select("calcRev(array(yuanbao))", sf.NET_AMT)
            select("level", sf.LEVEL)
            select("ref_trans_id", sf.TRANS_ID)
            select("os_platform_id", sf.DEVICE)
            from("hpt.recharge")
            where("error", "1")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
            registUdf("calcRev", calcRev)
            registUdf("extractServer", extractServer)
        }
        var rechargeConfig = new FormatterConfig(null, null, null, null)
        rechargeConfig.hiveQueryConfig = rechargeQuery
        rechargeConfig.inputFileType = Constants.INPUT_FILE_TYPE.HIVE

        //join
        var activityPath = getOutputPath(logDate, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER)
        var on: Array[String] = Array(sf.ID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sf.ID -> "is not null"))
        var fields: Map[String, Array[String]] = Map("A" -> Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.GROSS_AMT, sf.NET_AMT, sf.LEVEL, sf.TRANS_ID, sf.DEVICE),
            "B" -> Array(sf.OS, sf.OS_VERSION))
        var formatOS: FormatterConfig = new FormatterConfig(null, null, Array(sf.DEVICE, sf.OS, sf.OS_VERSION), activityPath, Map("type" -> "left join", "fields" -> fields, "where" -> where, "on" -> on))
        def customGenerate(dt:DataFrame): DataFrame = {
            dt.sort(dt("id"), dt("os").desc, dt("log_date").asc).dropDuplicates(Seq("id"))
        }
        formatOS.setCustomGenerate(customGenerate)
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

