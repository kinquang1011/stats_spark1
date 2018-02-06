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

object FarmWebFormatter extends IngameFormatter("sfgsn", "sfgsn") {
    
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
            return (arr.length == 14 && arr(0).startsWith(logDate) && arr(13) == "1")
        }
        
        def logoutFilter(arr: Array[String]): Boolean = {
            return (arr.length == 14 && arr(0).startsWith(logDate) && arr(13) == "1")
        }

        def loginGenerate(arr: Array[String]): Row = {
            
            Row(gameCode, arr(0), "login", arr(1), arr(5), arr(2), arr(4), arr(6), "0")
        }
        
        def logoutGenerate(arr: Array[String]): Row = {
            
            Row(gameCode, arr(0), "logout", arr(1), arr(5), arr(2), arr(4), arr(6), arr(12))
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.SID, sf.RID, sf.IP, sf.LEVEL, sf.ONLINE_TIME)
        var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.SID, sf.RID, sf.IP, sf.LEVEL, sf.ONLINE_TIME)

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
            return (arr.length == 15 && arr(3).startsWith(logDate))
        }

        def paymentGenerate(arr: Array[String]): Row = {

            Row(gameCode, arr(3), arr(0), arr(4), arr(14), arr(10), arr(11), arr(7))
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(
                sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.TRANS_ID, sf.GROSS_AMT, sf.NET_AMT, sf.PAY_CHANNEL)
        var paymentFormat = new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        
        var config: Array[FormatterConfig] = Array(
            paymentFormat
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
    
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
    }
}

