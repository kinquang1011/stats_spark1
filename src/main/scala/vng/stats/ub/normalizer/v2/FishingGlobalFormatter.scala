package vng.stats.ub.normalizer.v2

import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DateTimeUtils, DataUtils, Common, Constants}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import vng.stats.ub.normalizer.IngameFormatter
import scala.util.matching.Regex

object FishingGlobalFormatter extends IngameFormatter("ftgfbs2", "ftgfbs2") {
    
    /*
     * Raw log: GMT +8
     * 
     * Calculate from 23h yesterday to today at 23h
     */
    var extraTime = -3600000L
    
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
        
        var loginPathList = DataUtils.getMultiFileWithZone(loginPath, logDate, 1)
        var logoutPathList = DataUtils.getMultiFileWithZone(logoutPath, logDate, 1)
        
        val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        
        def loginFilter(arr: Array[String]): Boolean = {
            
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            return (newLogDate.startsWith(logDate) && arr(16) == "1")
        }

        def logoutFilter(arr: Array[String]): Boolean = {
            
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            return (newLogDate.startsWith(logDate) && arr(17) == "1")
        }

        def loginGenerate(arr: Array[String]): Row = {

            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            Row(gameCode, newLogDate, "login", arr(1), arr(2), arr(3), arr(10), "0", arr(8), arr(6), arr(7), arr(17))
        }
        
        def logoutGenerate(arr: Array[String]): Row = {

            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            Row(gameCode, newLogDate, "logout", arr(1), arr(2), arr(3), arr(10), arr(16), arr(18), arr(6), arr(7), arr(18))
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.SID, sf.RID, sf.LEVEL, sf.ONLINE_TIME, sf.DEVICE, sf.OS, sf.OS_VERSION, sf.DID)
        var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.SID, sf.RID, sf.LEVEL, sf.ONLINE_TIME, sf.DEVICE, sf.OS, sf.OS_VERSION, sf.DID)
        
        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(loginFilter, loginGenerate, loginMapping, loginPathList, Map("type" -> "union")),
            new FormatterConfig(logoutFilter, logoutGenerate, logoutMapping, logoutPathList, Map("type" -> "union"))
        )

        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }

    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var paymentPath = Common.getInputParquetPath(gameFolder, "ingame_recharge")
        var paymentPathList = DataUtils.getMultiFileWithZone(paymentPath, logDate, 1)
        val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        def paymentFilter(arr: Array[String]): Boolean = {
            
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            //return (arr(0).startsWith("2016-04-11 21:33") && arr(18) == "1")
            return (newLogDate.startsWith(logDate) && arr(18) == "1")
        }

        def paymentGenerate(arr: Array[String]): Row = {
            
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            Row(gameCode, newLogDate, arr(1), arr(2), arr(3), arr(19), arr(9), arr(10), arr(8), arr(7), arr(6))
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.LEVEL, sf.GROSS_AMT, sf.NET_AMT, sf.IP, sf.TRANS_ID, sf.PAY_CHANNEL)

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

