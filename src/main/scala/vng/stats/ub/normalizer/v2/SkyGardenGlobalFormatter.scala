package vng.stats.ub.normalizer.v2

import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DateTimeUtils, DataUtils, Common, Constants}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import vng.stats.ub.normalizer.IngameFormatter
import scala.util.matching.Regex

object SkyGardenGlobalFormatter extends IngameFormatter("cgmbgfbs1", "cgmbgfbs1") {
    
    var extraTime = 25200000L
    
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
        
        val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        
        def loginFilter(arr: Array[String]): Boolean = {
            
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            return (arr.length == 13 && newLogDate.startsWith(logDate))
        }

        def logoutFilter(arr: Array[String]): Boolean = {
            
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            return (arr.length == 13 && newLogDate.startsWith(logDate))
        }

        def loginGenerate(arr: Array[String]): Row = {

            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            Row(gameCode, newLogDate, "login", arr(1), arr(2), arr(5), arr(6), "0", arr(4), arr(12))
        }
        
        def logoutGenerate(arr: Array[String]): Row = {

            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            Row(gameCode, newLogDate, "logout", arr(1), arr(2), arr(5), arr(6), arr(12), arr(4), "")
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.LEVEL, sf.ONLINE_TIME, sf.IP, sf.DEVICE)
        var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.LEVEL, sf.ONLINE_TIME, sf.IP, sf.DEVICE)
        
        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(loginFilter, loginGenerate, loginMapping, loginPathList, Map("type" -> "union")),
            new FormatterConfig(logoutFilter, logoutGenerate, logoutMapping, logoutPathList, Map("type" -> "union"))
        )

        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }

    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var paymentPath = Common.getInputParquetPath(gameFolder, "payment")
        var paymentPathList = DataUtils.getMultiFiles(paymentPath, logDate, 1)
        val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        def paymentFilter(arr: Array[String]): Boolean = {
            
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            return (arr.length == 20 && newLogDate.startsWith(logDate) && arr(15) == "0" && arr(7) > "0")
        }

        def paymentGenerate(arr: Array[String]): Row = {
            
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            Row(gameCode, newLogDate, arr(1), arr(16), arr(18), arr(7), arr(7), arr(19), arr(5), arr(4), arr(3))
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.RID, sf.SID, sf.GROSS_AMT, sf.NET_AMT, sf.LEVEL, sf.IP, sf.TRANS_ID, sf.PAY_CHANNEL)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
    
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
    }
    
    override def roleRegisterFormatter(logDate: String, sc: SparkContext): Unit = {
        
        
    }
}

