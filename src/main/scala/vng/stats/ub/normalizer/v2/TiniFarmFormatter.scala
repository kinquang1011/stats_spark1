package vng.stats.ub.normalizer.v2

import scala.util.matching.Regex
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import vng.stats.ub.normalizer.IngameFormatter
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2.LoginLogoutFormatter
import vng.stats.ub.normalizer.format.v2.PaymentFormatter
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.normalizer.format.v2.CcuFormatter

object TiniFarmFormatter extends IngameFormatter("tfzfbs2", "tfzfbs2") {
    
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
            return (arr.length == 18 && arr(0).startsWith(logDate) && arr(17) == "1")
        }
        
        def logoutFilter(arr: Array[String]): Boolean = {
            return (arr.length == 19 && arr(0).startsWith(logDate) && arr(18) == "1")
        }

        def loginGenerate(arr: Array[String]): Row = {
            
            var versionRegex = new Regex("\\d([0-9\\.]*)")
            var osRegex = new Regex("(\\w*)")
            
            var version = versionRegex.findFirstIn(arr(6))
            var os = osRegex.findFirstIn(arr(6).toLowerCase())
            var device = arr(9).replace("Handheld|", "")
            
            Row(gameCode, arr(0), "login", arr(2), arr(1), arr(3), arr(11), device, arr(8), os, version, "0")
        }
        
        def logoutGenerate(arr: Array[String]): Row = {
            
            var versionRegex = new Regex("\\d([0-9\\.]*)")
            var osRegex = new Regex("(\\w*)")
            
            var version = versionRegex.findFirstIn(arr(6))
            var os = osRegex.findFirstIn(arr(6).toLowerCase())
            var device = arr(9).replace("Handheld|", "")
            
            Row(gameCode, arr(0), "logout", arr(2), arr(1), arr(3), arr(11), device, arr(8), os, version, arr(17))
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.SID, sf.ID, sf.RID, sf.LEVEL, sf.DEVICE, sf.DID, sf.OS, sf.OS_VERSION, sf.ONLINE_TIME)
        var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.SID, sf.ID, sf.RID, sf.LEVEL, sf.DEVICE, sf.DID, sf.OS, sf.OS_VERSION, sf.ONLINE_TIME)

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
            return (arr.length == 22 && arr(0).startsWith(logDate) && arr(18) == "1" && arr(9) != "0")
        }

        def paymentGenerate(arr: Array[String]): Row = {
            
            var ipRegex = new Regex("(\\d[0-9\\.]*)")
            var ip = ipRegex.findFirstIn(arr(8))
            
            Row(gameCode, arr(0), arr(2), arr(1), arr(3), arr(19), ip, arr(7), arr(9), arr(10), arr(6))
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.ID, sf.RID, sf.LEVEL, sf.IP, sf.TRANS_ID, sf.GROSS_AMT, sf.NET_AMT, sf.PAY_CHANNEL)
        var paymentFormat = new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        
        var config: Array[FormatterConfig] = Array(
            paymentFormat
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
    
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var ccuPath = Common.getInputParquetPath(gameFolder, "ccu")
        var ccuPathList = DataUtils.getMultiFiles(ccuPath, logDate, 1)

        def filter(arr: Array[String]): Boolean = {
            return (arr(0).startsWith(logDate))
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

