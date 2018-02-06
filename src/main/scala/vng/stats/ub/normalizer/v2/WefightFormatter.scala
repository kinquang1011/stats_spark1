package vng.stats.ub.normalizer.v2

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{Common, Constants, DataUtils, DateTimeUtils}

object WefightFormatter extends IngameFormatter("wefight", "wefight") {
    def main(args: Array[String]) {
        initParameters(args)
        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }

    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {

    }
    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {

        var loginPath = Common.getInputParquetPath(gameCode, "player_login")
        var logoutPath = Common.getInputParquetPath(gameCode, "player_logout")

        var loginPathList = DataUtils.getMultiFiles(loginPath, logDate, 1)
        var logoutPathList = DataUtils.getMultiFiles(logoutPath, logDate, 1)

        def loginFilter(arr: Array[String]): Boolean = {
            (arr.length == 36 && arr(2).startsWith(logDate))
        }

        def logoutFilter(arr: Array[String]): Boolean = {
            (arr.length == 34 && arr(2).startsWith(logDate))
        }

        def loginGenerate(arr: Array[String]): Row = {
            var os = "unknow"
            if (arr(10) == "1") {
                os = "android"
            } else if (arr(10) == "0") {
                os = "ios"
            }

            //ip
            var ip = ""
            if (arr(12) != "") {
                ip = arr(12).split(":")(0)
            }

            Row(gameCode, arr(2), "login", arr(4), arr(9), arr(8), arr(32), ip, "0", arr(28), os)
        }

        def logoutGenerate(arr: Array[String]): Row = {
            var os = "unknow"
            if (arr(10) == "1") {
                os = "android"
            } else if (arr(10) == "0") {
                os = "ios"
            }
            //ip
            var ip = ""
            if (arr(12) != "") {
                ip = arr(12).split(":")(0)
            }
            Row(gameCode, arr(2), "logout", arr(4), arr(9), arr(8), arr(33), ip, arr(29), arr(28), os)
        }

        var sF = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sF.GAME_CODE, sF.LOG_DATE, sF.ACTION, sF.SID, sF.ID, sF.RID, sF.LEVEL, sF.IP, sF.ONLINE_TIME, sF.DID, sF.OS)
        var logoutMapping: Array[String] = Array(sF.GAME_CODE, sF.LOG_DATE, sF.ACTION, sF.SID, sF.ID, sF.RID, sF.LEVEL, sF.IP, sF.ONLINE_TIME, sF.DID, sF.OS)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(loginFilter, loginGenerate, loginMapping, loginPathList, Map("type" -> "union")),
            new FormatterConfig(logoutFilter, logoutGenerate, logoutMapping, logoutPathList, Map("type" -> "union"))
        )

        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.format(sc)

    }

    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        var paymentPath = Common.getInputParquetPath(gameCode, "acc_water")
        var paymentPathList = DataUtils.getMultiFiles(paymentPath, logDate, 1)
        def paymentFilter(arr: Array[String]): Boolean = {
            (arr.length == 11
                && arr(0).startsWith(logDate)
                )
        }
        def paymentGenerate(arr: Array[String]): Row = {
            var n = (arr(10).toDouble * 200).toString
            var g = n
            Row(gameCode, arr(0), arr(2), arr(3), g, n, arr(1))
        }
        var sF = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sF.GAME_CODE, sF.LOG_DATE, sF.ID, sF.SID, sF.GROSS_AMT, sF.NET_AMT, sF.TRANS_ID)
        var paymentFormat = new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        paymentFormat.setCoalescePartition(1)

        var on: Array[String] = Array(sF.ID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sF.ID -> "is not null"))
        var fields: Map[String, Array[String]] = Map("A" -> Array(sF.GAME_CODE, sF.LOG_DATE, sF.ID, sF.SID, sF.GROSS_AMT, sF.NET_AMT, sF.TRANS_ID),
            "B" -> Array(sF.OS))
        var loginMapping: Array[String] = Array(sF.OS)
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

