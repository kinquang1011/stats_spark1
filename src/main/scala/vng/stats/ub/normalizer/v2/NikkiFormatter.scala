package vng.stats.ub.normalizer.v2

import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DateTimeUtils, DataUtils, Common, Constants}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import vng.stats.ub.normalizer.IngameFormatter

object NikkiFormatter extends IngameFormatter("nikki", "nikki") {

    def main(args: Array[String]) {
        
        initParameters(args)

        val conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        val sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }

    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        
        val loginPath = Common.getInputParquetPath(gameFolder, "login")
        val logoutPath = Common.getInputParquetPath(gameFolder, "logout")
        
        val loginPathList = DataUtils.getMultiFiles(loginPath, logDate, 1)
        val logoutPathList = DataUtils.getMultiFiles(logoutPath, logDate, 1)

        def loginFilter(arr: Array[String]): Boolean = {
            return (arr.length == 25 && arr(2).startsWith(logDate))
        }

        def logoutFilter(arr: Array[String]): Boolean = {
            return (arr.length == 29 && arr(2).startsWith(logDate))
        }

        def loginGenerate(arr: Array[String]): Row = {

            var device = ""
            var os = "ios"
            
            if(arr(4) == "1"){
                os = "android"
            }
            
            device = arr(11).split(",").apply(0)
            
            Row(gameCode, arr(2), "login", arr(6), "", arr(1), arr(17), arr(7), "", arr(22), "0", os, device)
        }
        
        def logoutGenerate(arr: Array[String]): Row = {

            var device = ""
            var os = "ios"
            
            if(arr(4) == "1"){
                os = "android"
            }
            
            device = arr(12).split(",").apply(0)
            
            Row(gameCode, arr(2), "logout", arr(6), "", arr(1), arr(18), arr(8), "", arr(23), arr(7), os, device)
        }

        val sf = Constants.LOGIN_LOGOUT_FIELD
        val loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.CHANNEL, sf.LEVEL, sf.IP, sf.DID, sf.ONLINE_TIME, sf.OS, sf.DEVICE)
        val logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.CHANNEL, sf.LEVEL, sf.IP, sf.DID, sf.ONLINE_TIME, sf.OS, sf.DEVICE)
        
        val config: Array[FormatterConfig] = Array(
            new FormatterConfig(loginFilter, loginGenerate, loginMapping, loginPathList, Map("type" -> "union")),
            new FormatterConfig(logoutFilter, logoutGenerate, logoutMapping, logoutPathList, Map("type" -> "union"))
        )

        val formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
    
    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        
        val paymentPath = Common.getInputParquetPath(gameFolder, "recharge")
        val paymentPathList = DataUtils.getMultiFiles(paymentPath, logDate, 1)

        def paymentFilter(arr: Array[String]): Boolean = {
            return (arr.length == 13 && arr(11).startsWith(logDate))
        }

        def paymentGenerate(arr: Array[String]): Row = {
            
            Row(gameCode, arr(11), arr(12), arr(5), arr(5), arr(1), arr(2))
        }

        val sf = Constants.PAYMENT_FIELD
        val paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT, sf.NET_AMT, sf.PAY_CHANNEL, sf.TRANS_ID)

        // Join: get os info
        val activityPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER, logDate, false)
        
        val on: Array[String] = Array(sf.ID)
        val where: Map[String, Map[String, Any]] = Map()
        val fields: Map[String, Array[String]] = Map("A" -> Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.GROSS_AMT, sf.NET_AMT, sf.PAY_CHANNEL, sf.TRANS_ID),
                "B" -> Array(sf.DEVICE, sf.OS, sf.OS_VERSION))

        val formatOS: FormatterConfig = new FormatterConfig(null, null, Array(sf.DEVICE, sf.OS, sf.OS_VERSION), activityPath, Map("type" -> "left join", "fields" -> fields, "where" -> where, "on" -> on))
        formatOS.setInputFileType("parquet")
        formatOS.setFieldDistinct(Array("id"))
        formatOS.setCoalescePartition(1)
        
        val config: Array[FormatterConfig] = Array(
            new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList),
            formatOS
        )

        val formatter = new PaymentFormatter(gameCode, logDate, config, false)
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
}

