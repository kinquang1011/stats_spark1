package vng.stats.ub.normalizer.v2

import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DateTimeUtils, DataUtils, Common, Constants}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import scala.util.Try
import vng.stats.ub.normalizer.IngameFormatter

object G10SeaFormatter extends IngameFormatter("stct", "g10sea") {

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
        
        var loginPath = Common.getInputParquetPath("g10sea", "login")
        var logoutPath = Common.getInputParquetPath("g10sea", "logout")
        
        var loginPathList = DataUtils.getMultiFiles(loginPath, logDate, 1)
        var logoutPathList = DataUtils.getMultiFiles(logoutPath, logDate, 1)

        val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        
        def loginFilter(arr: Array[String]): Boolean = {
            
            //var newLogDate = formatTime.format(formatTime.parse(arr(11)).getTime + extraTime)
            return (arr.length == 29 && arr(11).startsWith(logDate))
        }
        
        def logoutFilter(arr: Array[String]): Boolean = {
            
            //var newLogDate = formatTime.format(formatTime.parse(arr(21)).getTime + extraTime)
            return (arr.length == 41 && arr(11).startsWith(logDate))
        }

        def loginGenerate(arr: Array[String]): Row = {
            
            var device = ""
            if(arr(6).split("#").length > 1){
                device = arr(6).split("#").apply(1)
            }
            
            //var newLogDate = formatTime.format(formatTime.parse(arr(11)).getTime + extraTime)
            
            Row(gameCode, arr(11), "login", arr(23), arr(0), arr(20), arr(25), arr(2), arr(21), device, arr(17), arr(18), arr(8), "0")
        }
        
        def logoutGenerate(arr: Array[String]): Row = {
            
            var device = ""
            if(arr(9).split("#").length > 1){
                device = arr(9).split("#").apply(1)
            }
            
            //var newLogDate = formatTime.format(formatTime.parse(arr(21)).getTime + extraTime)
            
            Row(gameCode, arr(11), "logout", arr(36), arr(0), arr(33), arr(38), arr(3), arr(34), device, arr(27), arr(28), arr(16), arr(26))
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.SID, sf.ID, sf.RID, sf.DID, sf.CHANNEL, sf.LEVEL, sf.DEVICE, sf.OS, sf.OS_VERSION, sf.IP, sf.ONLINE_TIME)
        var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.SID, sf.ID, sf.RID, sf.DID, sf.CHANNEL, sf.LEVEL, sf.DEVICE, sf.OS, sf.OS_VERSION, sf.IP, sf.ONLINE_TIME)

        var loginFormatConfig = new FormatterConfig(loginFilter, loginGenerate, loginMapping, loginPathList, Map("type" -> "union"))
        var logoutFormatConfig = new FormatterConfig(logoutFilter, logoutGenerate, logoutMapping, logoutPathList, Map("type" -> "union"))
        
        //loginFormatConfig.setExtraTime(extraTime)
        //logoutFormatConfig.setExtraTime(extraTime)
        
        var config: Array[FormatterConfig] = Array(
            loginFormatConfig, logoutFormatConfig
        )
        
        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.format(sc)

    }
    
    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var paymentPath = Common.getInputParquetPath("g10sea", "recharge")
        var paymentPathList = DataUtils.getMultiFiles(paymentPath, logDate, 1)
        
        val convertRate = Map[String, Double]("THB" -> 484, "MYR" -> 4155, "IDR" -> 1.25, "AUD" -> 12441, "BRL" -> 5066, "INR" -> 255,
                "NZD" -> 11805, "PHP" -> 300, "SGD" -> 12431, "TWD" -> 450, "TRY" -> 5757, "EUR" -> 18575, "USD" -> 16755, "VND" -> 1)

        val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        
        def paymentFilter(arr: Array[String]): Boolean = {
            
            //var newLogDate = formatTime.format(formatTime.parse(arr(12)).getTime + extraTime)
            return (arr.length == 23 && arr(12).startsWith(logDate))
        }
        
        def paymentGenerate(arr: Array[String]): Row = {
            
            var rate = 6.0
            var revenue = 0.0
            var isExist = Try {
                rate = convertRate(arr(5))
            }
                
            if(isExist.isFailure){
                rate = 6.0
            }
            
            revenue = rate * arr(2).toDouble
            //var newLogDate = formatTime.format(formatTime.parse(arr(12)).getTime + extraTime)
            
            Row(gameCode, arr(12), arr(17), arr(0), arr(15), arr(20), arr(18), arr(10), revenue.toString, revenue.toString)
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.SID, sf.ID, sf.RID, sf.LEVEL, sf.TRANS_ID, sf.PAY_CHANNEL, sf.GROSS_AMT, sf.NET_AMT)
        
        var paymentFormat = new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        //paymentFormat.setExtraTime(extraTime)
        
        var config: Array[FormatterConfig] = Array(
            paymentFormat
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
    
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
    }
}

