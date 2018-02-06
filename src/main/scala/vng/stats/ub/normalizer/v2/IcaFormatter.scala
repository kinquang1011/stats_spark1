package vng.stats.ub.normalizer.v2

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.normalizer.v2.SgmbFormatter._
import vng.stats.ub.utils.{Common, Constants, DataUtils, DateTimeUtils}


object IcaFormatter extends IngameFormatter("icamfbs2", "ica") {

    val RECHARGE_SUCCESS = "1"

    def main(args: Array[String]) {
        initParameters(args)

        rechargeFolder = "ingame_recharge"

        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }

    //override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {}
    override def roleRegisterFormatter(logDate: String, sc: SparkContext): Unit = {}

    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var loginPath = Common.getInputParquetPath(gameFolder, loginFolder)
        var logoutPath = Common.getInputParquetPath(gameFolder, logoutFolder)
        
        var loginPathList = DataUtils.getMultiFiles(loginPath, logDate, 1)
        var logoutPathList = DataUtils.getMultiFiles(logoutPath, logDate, 1)

        def loginFilter(arr: Array[String]): Boolean = {
            (arr.length >= 18) &&
            (arr(0).startsWith(logDate))
        }
        
        def logoutFilter(arr: Array[String]): Boolean = {
            (arr.length >= 19) &&
            (arr(0).startsWith(logDate))
        }

        def loginGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(0), "login", arr(2), arr(1),arr(3),arr(10),arr(6),arr(7),arr(8),"0", arr(17))
        }
        
        def logoutGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(0), "logout", arr(2), arr(1),arr(3),arr(10),arr(6),arr(7),arr(8),arr(16), arr(18))
        }

        var sF = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sF.GAME_CODE, sF.LOG_DATE, sF.ACTION, sF.SID, sF.ID, sF.RID, sF.LEVEL, sF.OS, sF.OS_VERSION, sF.DEVICE, sF.ONLINE_TIME, sF.DID)
        var logoutMapping: Array[String] = Array(sF.GAME_CODE, sF.LOG_DATE, sF.ACTION, sF.SID, sF.ID, sF.RID, sF.LEVEL, sF.OS, sF.OS_VERSION, sF.DEVICE, sF.ONLINE_TIME, sF.DID)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(loginFilter, loginGenerate, loginMapping, loginPathList, Map("type" -> "union")),
            new FormatterConfig(logoutFilter, logoutGenerate, logoutMapping, logoutPathList, Map("type" -> "union"))
        )

        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.format(sc)

    }
    
    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var paymentPath = Common.getInputParquetPath(gameFolder, rechargeFolder)
        var paymentPathList = DataUtils.getMultiFiles(paymentPath, logDate, 1)
        def paymentFilter(arr: Array[String]): Boolean = {
            //var _logDate = DateTimeUtils.getDate(arr(0).toLong)
            (arr.length == 22
                && arr(0).startsWith(logDate)
                && arr(18) == RECHARGE_SUCCESS)
        }

        def paymentGenerate(arr: Array[String]): Row = {
            Row(gameCode, arr(0), arr(1), arr(2), arr(3), arr(9), arr(10), arr(19), arr(5), arr(6))
        }

        var sF = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sF.GAME_CODE, sF.LOG_DATE, sF.ID, sF.SID, sF.RID, sF.GROSS_AMT, sF.NET_AMT, sF.LEVEL, sF.CHANNEL, sF.PAY_CHANNEL)
        var paymentFormat = new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        paymentFormat.setCoalescePartition(1)

        var on: Array[String] = Array(sF.ID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sF.ID -> "is not null"))
        var fields: Map[String, Array[String]] = Map("A" -> Array(sF.GAME_CODE, sF.LOG_DATE, sF.ID, sF.SID, sF.RID, sF.GROSS_AMT, sF.NET_AMT, sF.LEVEL, sF.CHANNEL, sF.PAY_CHANNEL),
            "B" -> Array(sF.OS, sF.PACKAGE_NAME))
        var loginMapping: Array[String] = Array(sF.OS, sF.PACKAGE_NAME)
        var loginPathList = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER, logDate, false)
        var loginFormat: FormatterConfig = new FormatterConfig(null, null, loginMapping, loginPathList, Map("type" -> "left join", "fields" -> fields, "where" -> where, "on" -> on))
        loginFormat.setInputFileType("parquet")
        loginFormat.setFieldDistinct(Array("id"))
        loginFormat.setCoalescePartition(1)

        var config: Array[FormatterConfig] = Array(
            paymentFormat,
            loginFormat
        )
        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }

}

