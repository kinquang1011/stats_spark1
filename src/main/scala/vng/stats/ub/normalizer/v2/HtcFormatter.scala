package vng.stats.ub.normalizer.v2

import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DateTimeUtils, DataUtils, Common, Constants}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import scala.util.matching.Regex
import vng.stats.ub.utils.RegularExpression
import vng.stats.ub.normalizer.IngameFormatter

object HtcFormatter extends IngameFormatter("htc", "htc") {

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
            return (arr.length == 24 && arr(0).startsWith(logDate) && arr(23) == "1")
        }

        def logoutFilter(arr: Array[String]): Boolean = {
            return (arr.length == 21 && arr(8).startsWith(logDate) && arr(20) == "1")
        }

        def loginGenerate(arr: Array[String]): Row = {

            var ipRegex = new Regex("((?:[0-9]{1,3}\\.){3}[0-9]{1,3})")
            var ip = ipRegex.findFirstIn(arr(7))
            Row(gameCode, arr(0), "login", arr(5), arr(8), arr(6), arr(17), ip, arr(1), "0", arr(11), arr(13), arr(16))
        }
        
        def logoutGenerate(arr: Array[String]): Row = {

            Row(gameCode, arr(8), "logout", arr(2), arr(4), arr(1), arr(6), "", "", arr(9), arr(14), arr(11), arr(12))
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.LEVEL, sf.IP, sf.DID, sf.ONLINE_TIME, sf.DEVICE, sf.OS, sf.OS_VERSION)
        var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.LEVEL, sf.IP, sf.DID, sf.ONLINE_TIME, sf.DEVICE, sf.OS, sf.OS_VERSION)
        
        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(loginFilter, loginGenerate, loginMapping, loginPathList, Map("type" -> "union")),
            new FormatterConfig(logoutFilter, logoutGenerate, logoutMapping, logoutPathList, Map("type" -> "union"))
        )

        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }

    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var paymentPath = Common.getInputParquetPath(gameFolder, "recharge")
        var paymentPathList = DataUtils.getMultiFiles(paymentPath, logDate, 1)

        def paymentFilter(arr: Array[String]): Boolean = {
            return (arr.length == 33 && arr(0).startsWith(logDate))
        }

        def paymentGenerate(arr: Array[String]): Row = {
            
            var ipRegex = new Regex("((?:[0-9]{1,3}\\.){3}[0-9]{1,3})")
            var ip = ipRegex.findFirstIn(arr(18))
            
            Row(gameCode, arr(0), arr(6), arr(14), arr(7), arr(4), arr(21), arr(22), arr(9), arr(31), ip)
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.TRANS_ID, sf.RID, sf.SID, sf.GROSS_AMT, sf.NET_AMT, sf.PAY_CHANNEL, sf.LEVEL, sf.IP)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
    
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var ccuPath = Common.getInputParquetPath(gameFolder, "ccu")
        var ccuPathList = DataUtils.getMultiFiles(ccuPath, logDate, 1)

        def filter(arr: Array[String]): Boolean = {
            return (arr.length == 3 && arr(0).startsWith(logDate))
        }

        def generate(arr: Array[String]): Row = {
            Row(gameCode, arr(0), arr(1), arr(2))
        }

        var sf = Constants.CCU
        var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.CCU)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(filter, generate, mapping, ccuPathList)
        )

        var formatter = new CcuFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
}

