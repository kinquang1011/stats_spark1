package vng.stats.ub.normalizer.v2

import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DateTimeUtils, DataUtils, Common, Constants}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import vng.stats.ub.normalizer.IngameFormatter
import scala.util.matching.Regex

object CubeFarmGlobalFormatter extends IngameFormatter("cfgfbs1", "cfgfbs1") {
    
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
        var registerPath = Common.getInputParquetPath(gameFolder, "register")
        
        var loginPathList = DataUtils.getMultiFiles(loginPath, logDate, 1)
        var logoutPathList = DataUtils.getMultiFiles(logoutPath, logDate, 1)
        var registerPathList = DataUtils.getMultiFiles(registerPath, logDate, 1)

        var versionRegex = new Regex("\\d([0-9\\.]*)")
        var ipRegex = new Regex("(\\d[0-9\\.]*)")
        val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        
        def loginFilter(arr: Array[String]): Boolean = {
            
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            return (/*arr.length == 28 && */newLogDate.startsWith(logDate) && arr(17) == "1")
        }

        def logoutFilter(arr: Array[String]): Boolean = {
            
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            
            return (/*arr.length == 20 && */newLogDate.startsWith(logDate) && arr(18) == "1")
        }
        
        def registerFilter(arr: Array[String]): Boolean = {
            
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            
            return (/*arr.length == 20 && */newLogDate.startsWith(logDate) && arr(11) == "1")
        }

        def loginGenerate(arr: Array[String]): Row = {

            var version = versionRegex.findFirstIn(arr(7))
            
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            var ip = ipRegex.findFirstIn(arr(18))
            
            Row(gameCode, newLogDate, "login", arr(1), arr(3), arr(2), arr(11), arr(8), "0", arr(6), version, arr(9), ip)
        }
        
        def logoutGenerate(arr: Array[String]): Row = {

            var version = versionRegex.findFirstIn(arr(7))
            
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            
            Row(gameCode, newLogDate, "logout", arr(1), arr(3), arr(2), arr(11), arr(8), arr(17), arr(6), version, arr(9), "")
        }
        
        def registerGenerate(arr: Array[String]): Row = {

            var version = versionRegex.findFirstIn(arr(5))
            
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            
            Row(gameCode, newLogDate, "register", arr(1), "", arr(2), "0", arr(6), "0", arr(4), version, arr(7), "")
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.LEVEL, sf.DID, sf.ONLINE_TIME, sf.OS, sf.OS_VERSION, sf.DEVICE, sf.IP)
        var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.LEVEL, sf.DID, sf.ONLINE_TIME, sf.OS, sf.OS_VERSION, sf.DEVICE, sf.IP)
        var registerMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.LEVEL, sf.DID, sf.ONLINE_TIME, sf.OS, sf.OS_VERSION, sf.DEVICE, sf.IP)
        
        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(loginFilter, loginGenerate, loginMapping, loginPathList, Map("type" -> "union")),
            new FormatterConfig(logoutFilter, logoutGenerate, logoutMapping, logoutPathList, Map("type" -> "union")),
            new FormatterConfig(registerFilter, registerGenerate, registerMapping, registerPathList, Map("type" -> "union"))
        )

        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }

    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var paymentPath = Common.getInputParquetPath(gameFolder, "paying")
        var paymentPathList = DataUtils.getMultiFiles(paymentPath, logDate, 1)

        var activityPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER, logDate, false)
        var ipRegex = new Regex("(\\d[0-9\\.]*)")
        val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        
        def paymentFilter(arr: Array[String]): Boolean = {
            
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            
            return (/*arr.length == 21 && */newLogDate.startsWith(logDate) && arr(10) > "0" && arr(18) == "1")
        }

        def paymentGenerate(arr: Array[String]): Row = {
            
            var ip = ipRegex.findFirstIn(arr(8))
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            
            Row(gameCode, newLogDate, arr(1), arr(3), arr(2), arr(10), arr(10), arr(19), ip, arr(7))
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.RID, sf.SID, sf.GROSS_AMT, sf.NET_AMT, sf.LEVEL, sf.IP, sf.TRANS_ID)

        var on: Array[String] = Array(sf.ID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sf.ID -> "is not null"))
        var fields: Map[String, Array[String]] = Map("A" -> Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.RID, sf.SID, sf.GROSS_AMT, sf.NET_AMT, sf.LEVEL, sf.IP, sf.TRANS_ID),
                "B" -> Array(sf.DEVICE, sf.OS, sf.OS_VERSION))
        
        var formatOS: FormatterConfig = new FormatterConfig(null, null, Array(sf.DEVICE, sf.OS, sf.OS_VERSION), activityPath, Map("type" -> "left join", "fields" -> fields, "where" -> where, "on" -> on))
        formatOS.setInputFileType("parquet")
        formatOS.setFieldDistinct(Array("id"))
        formatOS.setCoalescePartition(1)
        
        var paymentConf = new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        paymentConf.setCoalescePartition(1)
        
        var config: Array[FormatterConfig] = Array(
            paymentConf, formatOS
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
    
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
    }
    
    override def roleRegisterFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var roleRegisterPath = Common.getInputParquetPath(gameFolder, "register")
        var roleRegisterPathList = DataUtils.getMultiFiles(roleRegisterPath, logDate, 1)

        var versionRegex = new Regex("\\d([0-9\\.]*)")
        val formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        
        def filter(arr: Array[String]): Boolean = {
            
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            
            return (/*arr.length == 28 && */newLogDate.startsWith(logDate))
        }

        def generate(arr: Array[String]): Row = {
            
            var version = versionRegex.findFirstIn(arr(7))
            var newLogDate = formatTime.format(formatTime.parse(arr(0)).getTime + extraTime)
            
            Row(gameCode, newLogDate, arr(1), arr(2), "", version, arr(4), arr(5))
        }

        var sf = Constants.ROLE_REGISTER_FIELD
        //var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.IP, sf.CHANNEL, sf.DEVICE, sf.OS, sf.OS_VERSION, sf.PACKAGE_NAME)
        var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.DEVICE, sf.OS, sf.OS_VERSION)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(filter, generate, mapping, roleRegisterPathList)
        )

        var formatter = new RoleRegisterFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
}

