package vng.stats.ub.normalizer.hcatalog

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DataUtils, Common, Constants}
import vng.stats.ub.utils.DateTimeUtils

object GnmFormatter extends IngameFormatter ("gnm", "gnm") {

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
            select("role_id", sf.RID)
            select("server_id", sf.SID)
            from(gameFolder + ".login")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
        }

        var loginConfig = new FormatterConfig(null, null, null, null)
        loginConfig.hiveQueryConfig = loginQuery
        loginConfig.inputFileType = Constants.INPUT_FILE_TYPE.HIVE

        //join with json sdk
        def loginJsonFilter(dataFrame: DataFrame): DataFrame = {
            dataFrame.filter(dataFrame("updatetime").contains(logDate))
        }
        def loginJsonGenerate(dataFrame: DataFrame): DataFrame = {
            dataFrame.selectExpr("'" + gameCode + "' as game_code",
                "updatetime as log_date", "package_name as package_name", "userID as id",
                "device_id as did", "type as channel", "'login' as action",
                "device as device", "device_os as os", "os as os_version")
        }
        var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.PACKAGE_NAME, sf.ID, sf.DID, sf.CHANNEL, sf.ACTION, sf.DEVICE, sf.OS, sf.OS_VERSION)
        var jsonFormatConfig = new JsonFormatConfig()
        jsonFormatConfig.filter = loginJsonFilter
        jsonFormatConfig.generate = loginJsonGenerate

        val fileName = Common.getGameCodeInFileName(gameCode, "loginlogout").toUpperCase + "_Login_InfoLog"
        val pathList1 = DataUtils.getMultiSdkLogFiles("/ge/gamelogs/sdk", logDate, fileName, 1)

        var on: Array[String] = Array(sf.ID)
        var where: Map[String, Map[String, Any]] = Map()
        var fields: Map[String, Array[String]] = Map(
            "A" -> Array(sf.GAME_CODE, sf.LOG_DATE, sf.RID, sf.ACTION, sf.SID, sf.ID),
            "B" -> Array(sf.PACKAGE_NAME, sf.CHANNEL, sf.DID, sf.DEVICE, sf.OS, sf.OS_VERSION)
        )

        var loginJsonConfig = new FormatterConfig(null, null, mapping, pathList1,
            Map("type" -> "left join", "fields" -> fields, "where" -> where, "on" -> on))
        loginJsonConfig.jsonFormatConfig = jsonFormatConfig
        loginJsonConfig.inputFileType = Constants.INPUT_FILE_TYPE.JSON
        loginJsonConfig.setFieldDistinct(Array("id"))

        var config: Array[FormatterConfig] = Array(
            loginConfig,
            loginJsonConfig
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
            select("money * 100", sf.GROSS_AMT)
            select("money * 100", sf.NET_AMT)
            from(gameFolder + ".recharge")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
        }
        var rechargeConfig = new FormatterConfig(null, null, null, null)
        rechargeConfig.hiveQueryConfig = rechargeQuery
        rechargeConfig.inputFileType = Constants.INPUT_FILE_TYPE.HIVE

        //join with json sdk
        def paymentJsonFilter(dataFrame: DataFrame): DataFrame = {
            dataFrame.filter(dataFrame("updatetime").contains(logDate))
        }

        def paymentJsonGenerate(dataFrame: DataFrame): DataFrame = {
            dataFrame.selectExpr("'" + gameCode + "' as game_code",
                "updatetime as log_date","package_name as package_name",
                "userID as id",
                "device_id as did", "device as device", "device_os as os")
        }
        var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.PACKAGE_NAME, sf.ID, sf.DID, sf.DEVICE, sf.OS )
        var payementFormatConfig = new JsonFormatConfig()
        payementFormatConfig.filter = paymentJsonFilter
        payementFormatConfig.generate = paymentJsonGenerate

        val fileName = Common.getGameCodeInFileName(gameCode, "loginlogout").toUpperCase + "_Login_InfoLog"
        val pathList1 = DataUtils.getMultiSdkLogFiles("/ge/gamelogs/sdk", logDate, fileName, 1)

        var on: Array[String] = Array(sf.ID)
        var where: Map[String, Map[String, Any]] = Map()
        var fields: Map[String, Array[String]] = Map(
            "A" -> Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.GROSS_AMT, sf.NET_AMT),
            "B" -> Array(sf.PACKAGE_NAME, sf.DID, sf.DEVICE, sf.OS)
        )

        var paymentJsonConfig = new FormatterConfig(null, null, mapping, pathList1,
            Map("type" -> "left join", "fields" -> fields, "where" -> where, "on" -> on))
        paymentJsonConfig.jsonFormatConfig = payementFormatConfig
        paymentJsonConfig.inputFileType = Constants.INPUT_FILE_TYPE.JSON
        paymentJsonConfig.setFieldDistinct(Array("id"))

        var config: Array[FormatterConfig] = Array(
            rechargeConfig,
            paymentJsonConfig
        )
        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.setOutputPath(getOutputPath(logDate, Constants.PARQUET_2.PAYMENT_OUTPUT_FOLDER))
        formatter.format(sc)
    }


    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
        val sf = Constants.CCU
        var partition = getPartition(logDate)
        val formatCcu = (params: Seq[Any]) => {
            val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            var logDate = params(0).toString

            var min = logDate.substring(14,16).toInt
            min = min - (min % 5)

            var newDate = DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH", logDate) + ":%02d:00".format(min)
            newDate
        }
        var ccuQuery = new HiveQueryConfig {
            select("'" + gameCode + "'", sf.GAME_CODE)
            select("formatCcu(array(log_date))", sf.LOG_DATE)
            select("server_id", sf.SID)
            select("ccu", sf.CCU)
            from(gameFolder + ".ccu")
            where("date_format(log_date, \"yyyy-MM-dd\")", logDate)
            where_in("ds", partition)
            registUdf("formatCcu", formatCcu)
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

