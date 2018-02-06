package vng.stats.ub.normalizer

import org.apache.spark.{SparkContext, SparkConf}
import vng.stats.ub.utils.{ Common, Constants}
import org.apache.spark.sql.{ Row}
import vng.stats.ub.normalizer.format.v2.PaymentFormatter
import vng.stats.ub.utils.DateTimeUtils
import vng.stats.ub.normalizer.format.v1.FormatterConfig
import vng.stats.ub.normalizer.format.v2.FirstChargeFormatter


object GlobalFormatter_bk {
    
    val gameCode = "global"
    
    def main(args: Array[String]) {
        var mapParameters: Map[String,String] = Map()
        for(x <- args){
            val xx = x.split("=")
            mapParameters += (xx(0) -> xx(1))
        }

        val logDate = mapParameters("logDate")
        val logType = mapParameters("logType")

        val conf = new SparkConf().setAppName("Global Formatter")
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        val sc = new SparkContext(conf)

        logType match {
            case Constants.PAYMENT_TYPE => {
             
                convertFormatter(logDate, sc)
                directFormatter(logDate, sc)
            }
            case Constants.FIRST_CHARGE_TYPE =>
                firstChargeFormatter(logDate,sc)
        }
        sc.stop()
    }

    def convertFormatter(logDate: String, sc: SparkContext): Unit = {
        
        val YMD = DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", logDate)
        val prevYMD = DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1))
        
        // zingxu_convert file
        val fwZingxuConvertFile = Common.getRawInputPath(gameCode, YMD, "FW", "FW_ZingXuConvert_" + YMD + ".csv.gz")
        val prevZingxuConvertFile = Common.getRawInputPath(gameCode, prevYMD, "FW", "FW_ZingXuConvert_" + prevYMD + ".csv.gz")
        
        // zingxu_convert_detail
        val fwZingxuConvertDetailFile = Common.getRawInputPath(gameCode, YMD, "FW", "ZingXuConvertDetail_" + YMD + ".csv.gz")
        val prevZingxuConvertDetailFile = Common.getRawInputPath(gameCode, prevYMD, "FW", "ZingXuConvertDetail_" + prevYMD + ".csv.gz")

        val lstConvertFile = fwZingxuConvertFile + "," + prevZingxuConvertFile
        val lstConvertDetailFile = fwZingxuConvertDetailFile + "," + prevZingxuConvertDetailFile
        
        // payment channel
        val channelFile = Common.getRawInputPath(gameCode, YMD, "FW", "PaymentChannel_" + YMD + ".csv.gz")
        
        // item
        val itemFile = Common.getRawInputPath(gameCode, "zingxu_item.csv")
        
        // product
        val productFile = Common.getRawInputPath(gameCode, "zingxu_product.csv")
        
        val sF = Constants.PAYMENT_FIELD
        
        /** convert */
        def convertFilter(arr: Array[String]): Boolean = {
            return (arr.length == 10 && arr(7).startsWith(logDate))
        }

        def convertGenerate(arr: Array[String]): Row = {
            Row(arr(0), arr(2), arr(3), arr(8), DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss", arr(7)))
        }

        val convertMapping: Array[String] = Array(sF.TRANS_ID, sF.ID, "item_id", sF.IP, sF.LOG_DATE)
        
        /** convert detail */
        def convertDetailFilter(arr: Array[String]): Boolean = {
            return (arr.length == 4)
        }

        def convertDetailGenerate(arr: Array[String]): Row = {
            Row(arr(0), (arr(2).toLong * 100).toString, (arr(2).toLong * 100).toString, arr(3))
        }

        val convertDetailMapping: Array[String] = Array("trans_id", sF.GROSS_AMT, sF.NET_AMT, "channel_id")

        /** channel */
        def channelFilter(arr: Array[String]): Boolean = {
            return (arr.length == 2)
        }

        def channelGenerate(arr: Array[String]): Row = {
            Row(arr(0), arr(1).toLowerCase())
        }

        val channelMapping: Array[String] = Array("channel_id", "channel")
        
        /** item */
        def itemFilter(arr: Array[String]): Boolean = {
            return true
        }

        def itemGenerate(arr: Array[String]): Row = {
            Row(arr(3), arr(0))
        }

        val itemMapping: Array[String] = Array("item_id", "pid")
        
        /** product */
        def productFilter(arr: Array[String]): Boolean = {
            return true
        }
        
        def productGenerate(arr: Array[String]): Row = {
            Row(arr(0), arr(2).toLowerCase())
        }
        
        val productMapping: Array[String] = Array("pid", "game_code")
        
        
        /** config */
        /** convert join convert_detail -> A */
        var on: Array[String] = Array("trans_id")
        var where: Map[String, Map[String, Any]] = Map("A" -> Map("trans_id" -> "is not null"))
        var fields: Map[String, Array[String]] = Map("A" -> Array("trans_id", sF.LOG_DATE, sF.ID, "item_id", sF.IP), "B" -> Array(sF.GROSS_AMT, sF.NET_AMT, "channel_id"))
        
        /** A join channel -> B */
        var onChannel: Array[String] = Array("channel_id")
        var whereChannel: Map[String, Map[String, Any]] = Map("A" -> Map("channel_id" -> "is not null"))
        var fieldsChannel: Map[String, Array[String]] = Map("A" -> Array("trans_id", sF.LOG_DATE, sF.ID, "item_id", sF.IP, sF.GROSS_AMT, sF.NET_AMT), "B" -> Array("channel"))
        
        /** B join item -> C */
        var onItem: Array[String] = Array("item_id")
        var whereItem: Map[String, Map[String, Any]] = Map("A" -> Map("item_id" -> "is not null"))
        var fieldsItem: Map[String, Array[String]] = Map("A" -> Array("trans_id", sF.LOG_DATE, sF.ID, sF.IP, sF.GROSS_AMT, sF.NET_AMT, "channel"), "B" -> Array("pid"))
        
        /** C join Product -> D */
        var onProduct: Array[String] = Array("pid")
        var whereProduct: Map[String, Map[String, Any]] = Map("A" -> Map("pid" -> "is not null"))
        var fieldsProduct: Map[String, Array[String]] = Map("A" -> Array("trans_id", sF.LOG_DATE, sF.ID, sF.IP, sF.GROSS_AMT, sF.NET_AMT, "channel"), "B" -> Array("game_code"))
        
        var convertConfig = new FormatterConfig(convertFilter, convertGenerate, convertMapping, lstConvertFile)
        var convertDetailConfig = new FormatterConfig(convertDetailFilter, convertDetailGenerate, convertDetailMapping, lstConvertDetailFile, Map("type" -> "join", "fields" -> fields, "where" -> where, "on" -> on))
        var channelConfig = new FormatterConfig(channelFilter, channelGenerate, channelMapping, channelFile, Map("type" -> "join", "fields" -> fieldsChannel, "where" -> whereChannel, "on" -> onChannel))
        var itemConfig = new FormatterConfig(itemFilter, itemGenerate, itemMapping, itemFile, Map("type" -> "join", "fields" -> fieldsItem, "where" -> whereItem, "on" -> onItem))
        var productConfig = new FormatterConfig(productFilter, productGenerate, productMapping, productFile, Map("type" -> "join", "fields" -> fieldsProduct, "where" -> whereProduct, "on" -> onProduct))
        
        convertConfig.setCharDelimiter('|')
        convertDetailConfig.setCharDelimiter('|')
        channelConfig.setCharDelimiter('|')
        itemConfig.setCharDelimiter(',')
        productConfig.setCharDelimiter(',')
        
        val config: Array[FormatterConfig] = Array(convertConfig, convertDetailConfig, channelConfig, itemConfig, productConfig)

        val formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
    
    def directFormatter(logDate: String, sc: SparkContext): Unit = {
        
        val YMD = DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", logDate)
        val prevYMD = DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1))
        
        // dbgend file
        // /ge/gamelogs/global/20160601/FW/data/stagingarea/payment/dbgend
        val dbgendFile = Common.getRawInputPath(gameCode, YMD, "FW", "data/stagingarea/payment/dbgend")
        val prevDbgendFile = Common.getRawInputPath(gameCode, prevYMD, "FW", "data/stagingarea/payment/dbgend")
        
        val lstDbgendFile = dbgendFile + "," + prevDbgendFile
        
        // dbgend_app
        val appFile = Common.getRawInputPath(gameCode, "dbgend_app.csv")
        
        val sF = Constants.PAYMENT_FIELD
        
        /** dbgend */
        def dbgendFilter(arr: Array[String]): Boolean = {
            return (arr.length == 35 && arr(0).startsWith(YMD) && arr(26).equals("SUCCESSFUL") && !arr(13).equals("7"))
        }

        def dbgendGenerate(arr: Array[String]): Row = {
            Row(arr(0), arr(1), arr(2), DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss", arr(20)), arr(16), arr(33))
        }

        val dbgendMapping: Array[String] = Array(sF.TRANS_ID, "appid", sF.ID, sF.LOG_DATE, sF.GROSS_AMT, sF.NET_AMT)
        
        /** app */
        def appFilter(arr: Array[String]): Boolean = {
            return true
        }
        
        def appGenerate(arr: Array[String]): Row = {
            Row(arr(0), arr(2).toLowerCase())
        }
        
        val appMapping: Array[String] = Array("appid", sF.GAME_CODE)
        
        /** config */
        /** dbgend join dbgend_app */
        var onProduct: Array[String] = Array("appid")
        var whereProduct: Map[String, Map[String, Any]] = Map("A" -> Map("appid" -> "is not null"))
        var fieldsProduct: Map[String, Array[String]] = Map("A" -> Array(sF.TRANS_ID, sF.ID, sF.LOG_DATE, sF.GROSS_AMT, sF.NET_AMT), "B" -> Array(sF.GAME_CODE))
        
        var dbgendConfig = new FormatterConfig(dbgendFilter, dbgendGenerate, dbgendMapping, lstDbgendFile)
        var appConfig = new FormatterConfig(appFilter, appGenerate, appMapping, appFile, Map("type" -> "join", "fields" -> fieldsProduct, "where" -> whereProduct, "on" -> onProduct))
        
        appConfig.setCharDelimiter(',')
        
        val config: Array[FormatterConfig] = Array(dbgendConfig, appConfig)

        val formatter = new PaymentFormatter(gameCode, logDate, config, false)
        formatter.setWriteMode("append")
        formatter.format(sc)
    }
    
    def firstChargeFormatter(logDate: String, sc: SparkContext): Unit = {
        
        val YMD = DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", logDate)
        val prevYMD = DateTimeUtils.formatDate("yyyy-MM-dd", "yyyyMMdd", DateTimeUtils.getDateDifferent(-1, logDate, Constants.TIMING, Constants.A1))
        
        val fwFirstChargeFile = Common.getRawInputPath(gameCode, YMD, "FW", "FW_FirstCharge_" + YMD + ".csv.gz")
        val prevFwFirstChargeFile = Common.getRawInputPath(gameCode, prevYMD, "FW", "FW_FirstCharge_" + prevYMD + ".csv.gz")
        
        val productFile = Common.getRawInputPath(gameCode, "zingxu_product.csv")
        
        val lstFirstChargeFile = fwFirstChargeFile + "," + prevFwFirstChargeFile

        def filter(arr: Array[String]): Boolean = {
            return (arr.length > 1 && arr(2).startsWith(logDate))
        }
        
        def productFilter(arr: Array[String]): Boolean = {
            return true
        }

        def generateFirstCharge(arr: Array[String]): Row = {
            Row(arr(0), arr(1), DateTimeUtils.formatDate("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss", arr(2)))
        }
        
        val mappingFirstCharge: Array[String] = Array("pid", "id", "log_date")
       
        def generateProduct(arr: Array[String]): Row = {
            Row(arr(0), arr(2).toLowerCase())
        }
        
        val mappingProduct: Array[String] = Array("pid", "game_code")
        
        // missing mapping to ub code
        
        var sF = Constants.FIRSTCHARGE_FIELD

        var on: Array[String] = Array("pid")
        var where: Map[String, Map[String, Any]] = Map("A" -> Map(sF.ID -> "is not null"))
        var fields: Map[String, Array[String]] = Map("B" -> Array(sF.GAME_CODE), "A" -> Array(sF.LOG_DATE, sF.ID))

        var configFirstCharge = new FormatterConfig(filter, generateFirstCharge, mappingFirstCharge, lstFirstChargeFile)
        var configProduct = new FormatterConfig(productFilter, generateProduct, mappingProduct, productFile, Map("type" -> "join", "fields" -> fields, "where" -> where, "on" -> on))
        configFirstCharge.setCharDelimiter('|')
        configProduct.setCharDelimiter(',')
        
        val config: Array[FormatterConfig] = Array(configFirstCharge, configProduct)

        val formatter = new FirstChargeFormatter(gameCode, logDate, config, false)
        formatter.format(sc)
    }
}
