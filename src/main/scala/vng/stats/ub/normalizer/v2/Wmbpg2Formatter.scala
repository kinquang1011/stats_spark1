package vng.stats.ub.normalizer.v2

import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DateTimeUtils, DataUtils, Common, Constants}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import scala.util.matching.Regex
import vng.stats.ub.utils.RegularExpression
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.IngameFormatter

object Wmbpg2Formatter extends IngameFormatter("wmb", "wmbpg2") {

    def main(args: Array[String]) {
        
        initParameters(args)

        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }

    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var loginPath = Common.getInputParquetPath(gameFolder, "login")
        var logoutPath = Common.getInputParquetPath(gameFolder, "logout")
        
        var loginPathList = DataUtils.getMultiFiles(loginPath, logDate, 1)
        var logoutPathList = DataUtils.getMultiFiles(logoutPath, logDate, 1)
        
        def loginFilter(arr: Array[String]): Boolean = {
            return (arr.length == 25 && arr(1).startsWith(logDate))
        }

        def logoutFilter(arr: Array[String]): Boolean = {
            return (arr.length == 25 && arr(1).startsWith(logDate))
        }

        def loginGenerate(arr: Array[String]): Row = {

            var osRegex = new Regex("(android|iphone)")
            var os = osRegex.findFirstIn(arr(7).toLowerCase())
            var versionRegex = new Regex("(\\d+(?:\\.\\d+)+)")
            var version = versionRegex.findFirstIn(arr(7))
            Row(gameCode, arr(1), "login", arr(5), arr(19),arr(21), arr(13), arr(18), os, version)
        }
        
        def logoutGenerate(arr: Array[String]): Row = {

            var osRegex = new Regex("(android|iphone)")
            var os = osRegex.findFirstIn(arr(7).toLowerCase())
            var versionRegex = new Regex("(\\d+(?:\\.\\d+)+)")
            var version = versionRegex.findFirstIn(arr(7))
            
            Row(gameCode, arr(1), "logout", arr(5), arr(19), arr(21), arr(13), arr(18), os, version)
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.LEVEL, sf.CHANNEL, sf.DEVICE, sf.OS, sf.OS_VERSION)
        var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.LEVEL, sf.CHANNEL, sf.DEVICE, sf.OS, sf.OS_VERSION)
        
        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(loginFilter, loginGenerate, loginMapping, loginPathList, Map("type" -> "union")),
            new FormatterConfig(logoutFilter, logoutGenerate, logoutMapping, logoutPathList, Map("type" -> "union"))
        )

        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }

    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var paymentPath = Common.getInputParquetPath("wmbpg2", "recharge")
        var paymentPathList = DataUtils.getMultiFiles(paymentPath, logDate, 1)

        def paymentFilter(arr: Array[String]): Boolean = {
            return (arr.length == 18 && arr(1).startsWith(logDate))
        }

        def paymentGenerate(arr: Array[String]): Row = {
            var os = "ios"
            if(arr(3) == "1"){
                os = "android"
            }
            Row(gameCode, arr(1), arr(5), arr(7), arr(14), arr(14), arr(9), arr(11), os)
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.RID, sf.GROSS_AMT, sf.NET_AMT, sf.LEVEL, sf.IP, sf.OS)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }

    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
        
    }
}

