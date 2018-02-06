package vng.stats.ub.normalizer.v2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import vng.stats.ub.normalizer.format.v2._
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.utils.DateTimeUtils
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.IngameFormatter

object StonySeaFormatter extends IngameFormatter("stonysea", "stonysea") {

    def main(args: Array[String]) {
        
        initParameters(args)

        var conf = new SparkConf().setAppName(gameCode.toUpperCase + " Formatter")
        conf.set("spark.hadoop.varidateOutputSpecs", "false")
        var sc = new SparkContext(conf)
        run(sc)
        sc.stop()
    }
    
    override def loginLogoutFormatter(logDate: String, sc: SparkContext): Unit = {

    }
    
    override def paymentFormatter(logDate: String, sc: SparkContext): Unit = {
        
    }
    
    override def accRegisterFormatter(logDate: String, sc: SparkContext): Unit = {

    }
    
    override def firstChargeFormatter(logDate: String, sc: SparkContext): Unit = {

    }

    override def ccuFormatter(logDate: String, sc: SparkContext): Unit = {
        
        var ccuPath = Common.getInputParquetPath(gameCode, "ccu")
        var ccuPathList = DataUtils.getMultiFiles(ccuPath, logDate, 1)

        def filter(arr: Array[String]): Boolean = {
            return (arr.length == 3 && arr(0).startsWith(logDate))
        }

        def generate(arr: Array[String]): Row = {
            Row(gameCode, arr(0), arr(1), arr(2))
        }

        var sF = Constants.CCU
        var mapping: Array[String] = Array(sF.GAME_CODE, sF.LOG_DATE, sF.SID, sF.CCU)

        var config: Array[FormatterConfig] = Array(
            new FormatterConfig(filter, generate, mapping, ccuPathList)
        )

        var formatter = new CcuFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
}

