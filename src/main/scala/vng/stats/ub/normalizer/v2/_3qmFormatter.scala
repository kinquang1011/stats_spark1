package vng.stats.ub.normalizer.v2

import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DateTimeUtils, DataUtils, Common, Constants}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import scala.util.matching.Regex
import vng.stats.ub.normalizer.IngameFormatter

object _3qmFormatter extends IngameFormatter("3qmobile", "3qmobile") {

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
        
        var channelRegex = new Regex("^(?:(?![i])[a-z])*")

        def loginFilter(arr: Array[String]): Boolean = {
            return (arr.length == 24 && arr(2).startsWith(logDate))
        }

        def logoutFilter(arr: Array[String]): Boolean = {
            return (arr.length == 16 && arr(2).startsWith(logDate))
        }

        def loginGenerate(arr: Array[String]): Row = {
            
            var channel = channelRegex.findFirstIn(arr(6).toLowerCase())
            
            Row(gameCode, arr(2), "login", arr(6), arr(5), arr(1), channel, arr(7), arr(10), arr(16), arr(15), arr(20), arr(21), "0")
        }
        
        def logoutGenerate(arr: Array[String]): Row = {

            var channel = channelRegex.findFirstIn(arr(6).toLowerCase())
            Row(gameCode, arr(2), "logout", arr(6), arr(5), arr(1), channel, arr(12), arr(7), "", "", "", "", arr(10))
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.CHANNEL, sf.LEVEL, sf.IP, sf.DEVICE, sf.OS, sf.OS_VERSION, sf.DID, sf.ONLINE_TIME)
        var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.CHANNEL, sf.LEVEL, sf.IP, sf.DEVICE, sf.OS, sf.OS_VERSION, sf.DID, sf.ONLINE_TIME)
        
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

        var channelRegex = new Regex("^(?:(?![i])[a-z])*")
        
        def paymentFilter(arr: Array[String]): Boolean = {
            return (arr.length >= 17 && arr(2).startsWith(logDate))
        }

        def paymentGenerate(arr: Array[String]): Row = {
            
            var channel = channelRegex.findFirstIn(arr(6).toLowerCase())
            
            Row(gameCode, arr(2), arr(6), arr(5), arr(1), arr(10), arr(10), channel)
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.RID, sf.SID, sf.GROSS_AMT, sf.NET_AMT, sf.CHANNEL)

        // Join: get os info
        var activityPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER, logDate, false)
        
        var on: Array[String] = Array(sf.ID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sf.ID -> "is not null"))
        var fields: Map[String, Array[String]] = Map("A" -> Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.RID, sf.SID, sf.GROSS_AMT, sf.NET_AMT, sf.CHANNEL),
                "B" -> Array(sf.DEVICE, sf.OS, sf.OS_VERSION))

        var formatOS: FormatterConfig = new FormatterConfig(null, null, Array(sf.DEVICE, sf.OS, sf.OS_VERSION), activityPath, Map("type" -> "left join", "fields" -> fields, "where" -> where, "on" -> on))
        formatOS.setInputFileType("parquet")
        formatOS.setCoalescePartition(1)
        def customGenerate(dt:DataFrame): DataFrame = {
            dt.sort(dt("id"), dt("os").desc, dt("log_date").asc).dropDuplicates(Seq("id"))
        }
        formatOS.setCustomGenerate(customGenerate)

        var paymentConf = new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList)
        paymentConf.setCoalescePartition(1)
        
        var config: Array[FormatterConfig] = Array(
            paymentConf, formatOS
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
    
    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var ccuPath = Common.getInputParquetPath(gameFolder, "ccu")
        var ccuPathList = DataUtils.getMultiFiles(ccuPath, logDate, 1)

        def filter(arr: Array[String]): Boolean = {
            return (arr.length == 3 && arr(0).startsWith(logDate) && arr(1) != "IOS" && arr(1) != "ANDROID")
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
    
    override def roleRegisterFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var registerPath = Common.getInputParquetPath(gameFolder, "register")
        var registerPathList = DataUtils.getMultiFiles(registerPath, logDate, 1)

        var channelRegex = new Regex("^(?:(?![i])[a-z])*")
        
        def filter(arr: Array[String]): Boolean = {
            return (arr.length == 24 && arr(2).startsWith(logDate))
        }

        def generate(arr: Array[String]): Row = {
            
            var channel = channelRegex.findFirstIn(arr(6).toLowerCase())
            
            Row(gameCode, arr(2), arr(6), arr(1), arr(5), channel, arr(10), arr(15), arr(20), arr(16), arr(21))
        }

        var sf = Constants.ROLE_REGISTER_FIELD
        //var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.IP, sf.CHANNEL, sf.DEVICE, sf.OS, sf.OS_VERSION, sf.PACKAGE_NAME)
        var mapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.SID, sf.RID, sf.CHANNEL, sf.IP, sf.OS, sf.OS_VERSION, sf.DEVICE, sf.DID)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(filter, generate, mapping, registerPathList)
        )

        var formatter = new RoleRegisterFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
}

