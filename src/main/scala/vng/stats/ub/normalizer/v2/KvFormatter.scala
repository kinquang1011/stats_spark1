package vng.stats.ub.normalizer.v2

import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.normalizer.format.v1._
import vng.stats.ub.normalizer.format.v2.CcuFormatter
import vng.stats.ub.normalizer.format.v2.FirstChargeFromPaying
import vng.stats.ub.normalizer.format.v2.LoginLogoutFormatter
import vng.stats.ub.normalizer.format.v2.NewAccFromLogin
import vng.stats.ub.normalizer.format.v2.PaymentFormatter
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.{DateTimeUtils, DataUtils, Common, Constants}
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import vng.stats.ub.normalizer.IngameFormatter

object KvFormatter extends IngameFormatter("kv", "kv") {
    
    def main(args: Array[String]) {
        
        initParameters(args)

        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }

    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {
        var path1 = Common.getInputParquetPath(gameFolder,"login_logout")
        var path2 = Common.getInputParquetPath(gameFolder,"login_logout")


        var pathList1 = DataUtils.getMultiFiles(path1,logDate,1)
        var pathList2 = DataUtils.getMultiFiles(path2,logDate,1)

        def filter1(arr: Array[String]): Boolean = {
            return (
                (arr(5).startsWith(logDate))
                )
        }

        def filter2(arr: Array[String]): Boolean = {
            return (
                (arr(7).startsWith(logDate))
                )
        }

        def generate1(arr: Array[String]): Row = {
            Row("kv", arr(5), "login", arr(4), arr(1), arr(15), arr(6), "0", arr(14))
        }

        def generate2(arr: Array[String]): Row = {
            Row("kv", arr(7), "logout", arr(4), arr(1), arr(15), arr(8), arr(9), arr(14))
        }

        var sf = Constants.LOGIN_LOGOUT_FIELD
        var mapping1: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.LEVEL, sf.ONLINE_TIME, sf.IP)
        var mapping2: Array[String] = Array(sf.GAME_CODE, sf.LOG_DATE, sf.ACTION, sf.ID, sf.RID, sf.SID, sf.LEVEL, sf.ONLINE_TIME, sf.IP)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(filter1, generate1, mapping1, pathList1, Map("type" -> "union")),
            new FormatterConfig(filter2, generate2, mapping2, pathList2, Map("type" -> "union"))
        )

        var formatter = new LoginLogoutFormatter(gameCode, logDate, config, false)
        formatter.format(sc)

    }

    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        var path = Common.getInputParquetPath(gameFolder,"recharge")

        var pathList = DataUtils.getMultiFiles(path,logDate,1)


        def filter(arr: Array[String]): Boolean = {
            return (
                /*(arr(8) == Constants.KV.RECHARGE_SUCCESS) //thanh cong
                    && */ arr(1).startsWith(Constants.KV.DBG_ADD_PREFIX)
                    && (arr(0).startsWith(logDate))
                )
        }

        def generate(arr: Array[String]): Row = {
            Row("kv", arr(13), arr(0), arr(3), arr(9), (arr(7).toDouble*100).toString, (arr(7).toDouble*100).toString)
        }

        var sf = Constants.PAYMENT_FIELD
        var mapping: Array[String] = Array(sf.GAME_CODE, sf.SID, sf.LOG_DATE, sf.ID, sf.LEVEL, sf.GROSS_AMT, sf.NET_AMT)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(filter, generate, mapping, pathList, Map("type" -> "union"))
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

