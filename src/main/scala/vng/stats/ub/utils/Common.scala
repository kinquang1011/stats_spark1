package vng.stats.ub.utils

import java.text.SimpleDateFormat
import java.util.{Locale, Calendar}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext
import scala.util.Try
import scala.collection.mutable
import scala.util.control.Exception._
import net.liftweb.json.JsonAST._
import net.liftweb.json.Extraction._
import net.liftweb.json.Printer._


/**
 * Created by tuonglv on 10/05/2016.
 */
object Common {
    def isDoubleNumber(s: Any): Boolean = (allCatch opt s.toString.toDouble).isDefined

    def isLongNumber(s: Any): Boolean = (allCatch opt s.toString.toLong).isDefined

    def isIntegerNumber(s: Any): Boolean = (allCatch opt s.toString.toInt).isDefined

    def getFileObject(dates: List[String], path: String): Unit = {
        var listPath = List[String]()
        for (date <- dates) {
            val _path = path + "/" + date + "/part*"
            listPath = _path :: listPath
        }
    }

    def getRawInputPath(gameCode: String, pathElements: String*): String = {

        var path = Constants.GAMELOG_DIR + "/" + gameCode
        pathElements.foreach {
            p => path = path + "/" + p
        }

        path
    }



    def getInputParquetPath(gameCode: String, folderName: String): String = {
        val path = Constants.HOME_DIR + "/" + gameCode + "/" + folderName
        path
    }

    def touchFile(path: String, sc: SparkContext): Unit = {
        val data_arr = new Array[String](1)
        data_arr(0) = "flag"
        Try{
            sc.parallelize(data_arr).coalesce(1).saveAsTextFile(path)
        }
    }

    def getHourlyOutputParquetPath(gameCode: String, folderName:String, logDate: String,numberOfFiles:Int = 0): String = {
        val sb = new StringBuilder
        sb.append("{" + logDate)
        for (i <- 1 to numberOfFiles) {
            val date = DateTimeUtils.getDateDifferent(i * -1, logDate, Constants.TIMING, Constants.A1)
            sb.append("," + date)
        }
        sb.append("}")
        var rs = ""
        if (numberOfFiles == 0) {
            rs = Constants.HOME_DIR + "/" + gameCode + "/ub/sdk_data_hourly/" + folderName + "/" + logDate
        } else {
            rs = Constants.HOME_DIR + "/" + gameCode + "/ub/sdk_data_hourly/" + folderName + "/" + sb.toString()
        }
        rs
    }

    def isFolderExist(folderPath: String): Boolean= {
        val fs = FileSystem.get(new Configuration(true));
        var isExist = true
        if (!fs.exists(new Path(folderPath))) {
            isExist = false
        }
        isExist
    }

    def getOuputParquetPath(gameCode: String, folderName:String, logDate: String, isSdkLog: Boolean): String ={
        var ub_data="data"
        if(isSdkLog){
            ub_data="sdk_data"
        }
        val outputPath = Constants.HOME_DIR + "/" + gameCode + "/ub/" + ub_data + "/" + folderName + "/" + logDate
        outputPath
    }

    def getBehaviourReportPath(gameCode: String, folderName:String, logDate: String, isSdkLog: Boolean): String ={
        var ub_report="report"
        if(isSdkLog){
            ub_report="sdk_report"
        }
        val outputPath = Constants.HOME_DIR + "/" + gameCode + "/ub/" + ub_report + "/behaviour/" + folderName + "/" + logDate
        outputPath
    }


    def geInputSdkRawLogPath(inputPath: String = "") : String = {
        if(inputPath=="")
            return "/ge/gamelogs/sdk"
        return inputPath
    }

    def getGameCodeInFileName(gameCode: String, logType: String): String ={
        var m:String=""
        if(gameCode=="pv3d"){
            m="pv"
        } else if(gameCode == "ddd2mp2"){
            m="gunga"
        }else{
            if(gameCode=="tlbbm" && logType=="loginlogout"){
                m="tlbb"
            }else{
                m=gameCode
            }
        }
        m
    }

    def logger(message: String, level: String = "INFO"): Unit ={
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val calendar = Calendar.getInstance(Locale.UK)
        val date = calendar.getTime()
        val now = format.format(date)
        println(now + " UB-" + level + " " + message)
    }

    def hashMapToJson(hashMap: Object): String ={
        implicit val formats = net.liftweb.json.DefaultFormats
        var rs = compact(render(decompose(hashMap)))
        rs
    }
}
