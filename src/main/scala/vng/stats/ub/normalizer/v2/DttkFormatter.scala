package vng.stats.ub.normalizer.v2

import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DateTimeUtils, DataUtils, Common, Constants}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import scala.util.matching.Regex
import vng.stats.ub.normalizer.IngameFormatter

object DttkFormatter extends IngameFormatter("dttk", "dttk") {

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
            return (arr.length == 17 && arr(0).startsWith(logDate) && arr(16) == "1")
        }

        def logoutFilter(arr: Array[String]): Boolean = {
            return (arr.length == 18 && arr(0).startsWith(logDate) && arr(17) == "1")
        }

        def loginGenerate(arr: Array[String]): Row = {

            Row(gameCode, arr(0), "login", arr(1), arr(3), arr(2), arr(10), arr(8), arr(6), arr(7), "0")
        }

        def logoutGenerate(arr: Array[String]): Row = {

            Row(gameCode, arr(0), "logout", arr(1), arr(3), arr(2), arr(10), arr(8), arr(6), arr(7), arr(16))
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var loginMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.LEVEL, sf.DEVICE, sf.OS, sf.OS_VERSION, sf.ONLINE_TIME)
        var logoutMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.LEVEL, sf.DEVICE, sf.OS, sf.OS_VERSION, sf.ONLINE_TIME)

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
        
        def paymentFilter(arr: Array[String]): Boolean = {
            return (arr.length == 20 && arr(0).startsWith(logDate) && arr(18) == "1")
        }

        def paymentGenerate(arr: Array[String]): Row = {

            Row(gameCode, arr(0), arr(1), arr(3), arr(2), arr(10), arr(10), arr(19), arr(7), arr(8), arr(6))
        }

        var sf = Constants.PAYMENT_FIELD
        var paymentMapping: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.RID, sf.SID, sf.GROSS_AMT, sf.NET_AMT, sf.LEVEL, sf.TRANS_ID, sf.IP, sf.PAY_CHANNEL)

        // Join: get os info
        var activityPath = Common.getOuputParquetPath(gameCode, Constants.PARQUET_2.LOGIN_LOGOUT_OUTPUT_FOLDER, logDate, false)
        
        var on: Array[String] = Array(sf.ID)
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sf.ID -> "is not null"))
        var fields: Map[String, Array[String]] = Map("A" -> Array(sf.GAME_CODE, sf.LOG_DATE, sf.ID, sf.RID,sf.SID, sf.IP, sf.GROSS_AMT, sf.NET_AMT, sf.LEVEL, sf.TRANS_ID, sf.IP, sf.PAY_CHANNEL),
                "B" -> Array(sf.DEVICE, sf.OS, sf.OS_VERSION))

        var formatOS: FormatterConfig = new FormatterConfig(null, null, Array(sf.DEVICE, sf.OS, sf.OS_VERSION), activityPath, Map("type" -> "left join", "fields" -> fields, "where" -> where, "on" -> on))
        formatOS.setInputFileType("parquet")
        formatOS.setFieldDistinct(Array("id"))
        formatOS.setCoalescePartition(1)
        
        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(paymentFilter, paymentGenerate, paymentMapping, paymentPathList),
            formatOS
        )

        var formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
}

