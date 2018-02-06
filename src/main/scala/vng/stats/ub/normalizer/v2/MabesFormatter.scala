package vng.stats.ub.normalizer.v2

import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DateTimeUtils, DataUtils, Common, Constants}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import vng.stats.ub.normalizer.IngameFormatter
import scala.util.matching.Regex

object MabesFormatter extends IngameFormatter("10ha7ifbs1", "mabes") {
    
    var extraTime = 0L
    
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
            
            return (arr(0).startsWith(logDate))
        }

        def logoutFilter(arr: Array[String]): Boolean = {
            
            return (arr(0).startsWith(logDate))
        }

        def loginGenerate(arr: Array[String]): Row = {

            Row(gameCode, arr(0), "login", arr(1), arr(5), arr(6), "0", arr(4), arr(14), arr(20), arr(21))
        }
        
        def logoutGenerate(arr: Array[String]): Row = {

            Row(gameCode, arr(0), "logout", arr(1), arr(5), arr(6), arr(12), arr(4), "", "", "")
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.SID, sf.LEVEL, sf.ONLINE_TIME, sf.IP, sf.DEVICE, sf.OS, sf.OS_VERSION)
        var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.SID, sf.LEVEL, sf.ONLINE_TIME, sf.IP, sf.DEVICE, sf.OS, sf.OS_VERSION)
        
        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(loginFilter, loginGenerate, loginMapping, loginPathList, Map("type" -> "union")),
            new FormatterConfig(logoutFilter, logoutGenerate, logoutMapping, logoutPathList, Map("type" -> "union"))
        )

        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }

    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var paymentPath = Common.getInputParquetPath(gameFolder, "paying")
        var paymentPathList = DataUtils.getMultiFiles(paymentPath, logDate, 1)
        val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        def paymentFilter(arr: Array[String]): Boolean = {
            
            return (arr.length > 19 && arr(0).startsWith(logDate) && (arr(2) == "TOPUP" || arr(2) == "APPLE" || arr(2) == "GOOGLE") && arr(15) == "0")
        }

        def paymentGenerate(arr: Array[String]): Row = {
            
            Row(gameCode, arr(0), arr(1), arr(18), arr(9), arr(9), arr(19), arr(5), arr(4))
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.GROSS_AMT, sf.NET_AMT, sf.LEVEL, sf.IP, sf.TRANS_ID)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
    
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        val ccuPath = Common.getInputParquetPath(gameFolder, "ccu")
        val ccuPathList = DataUtils.getMultiFiles(ccuPath, logDate, 1)

        def filter(arr: Array[String]): Boolean = {
            return (arr.length == 3 && arr(0).startsWith(logDate))
        }

        def generate(arr: Array[String]): Row = {
            Row(gameCode, arr(0), arr(1), arr(2))
        }

        val sf = Constants.CCU
        val mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.CCU)

        val config: Array[FormatterConfig] = Array(
            new FormatterConfig(filter, generate, mapping, ccuPathList)
        )

        val formatter = new CcuFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
    
    override def roleRegisterFormatter(logDate: String, sc: SparkContext): Unit = {
        
        
    }
}

