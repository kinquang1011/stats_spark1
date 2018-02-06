package vng.stats.ub.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.util.Calendar
import java.util.Locale
import java.text.SimpleDateFormat

import util.control.Breaks._
import vng.stats.ub.common.DbFieldName

object DataUtils {

    /************************************* FROM V1  ********************************************************/

    /**
     * Get all file for each timing from logDate
     *
     * EX: get 7 previous files from 2016-05-19 (timing = a7)
     */
    def getListFiles(filePath: String, logDate: String, timing: String): List[String] = {

        var calculateMethod = DateTimeUtils.resolveCalculateMethod(timing);
        var lists = List[String]()

        calculateMethod match {
            case Constants.KPI => {

                lists = getListFilePathsByKPI(filePath, logDate, timing)
            }

            case Constants.TIMING => {

                lists = getListFilePathsByTiming(filePath, logDate, timing)
            }
        }

        lists
    }

    /**
     * Get all file for each timing from the day before/after logDate a number of "distance" day
     *
     * EX: get 7 previous files from 2016-05-19 with distance = 3 (timing = a7)
     * => get 7 previous files from 2016-05-16
     */
    def getListFiles(filePath: String, logDate: String, timing: String, distance: Integer): List[String] = {

        var calculateMethod = DateTimeUtils.resolveCalculateMethod(timing);
        var lists = List[String]()
        var newLogDate = DateTimeUtils.getDateDifferent(distance, logDate, calculateMethod, timing)

        calculateMethod match {
            case Constants.KPI => {

                lists = getListFilePathsByKPI(filePath, newLogDate, timing)
            }

            case Constants.TIMING => {

                lists = getListFilePathsByTiming(filePath, newLogDate, timing)
            }
        }

        lists
    }

    /**
     * Get n file before logDate (n = numberOfFiles)
     */
    def getNFilesBeforeDate(filePath: String, logDate: String, numberOfFiles: Integer): List[String] = {

        var lists = List[String]()
        val listDates = DateTimeUtils.getListDateBefore(numberOfFiles, logDate)
        listDates.foreach {
            date => lists = s"""$filePath/$date""" :: lists
        }
        lists
    }

    def getKpiId(calcId: String, key: String, timing: String): Integer = {

        var kpiId = 0

        /*
         * old version with device_id vs id
         * 
        if(calcId == Constants.ID){

            kpiId  = DbFieldName.KpiIds.apply(key).apply(0) + DbFieldName.Timings.apply(timing)
        }else{

            kpiId  = DbFieldName.KpiIds.apply(key).apply(1) + DbFieldName.Timings.apply(timing)
        }*/
        
        kpiId  = DbFieldName.KpiIds.apply(key).apply(0) + DbFieldName.Timings.apply(timing)

        kpiId
    }
    
    def getParameters(args: Array[String]):Map[String, String] = {
        
        var mapParameters: Map[String,String] = Map()
        for(x <- args){
            val xx = x.split("=")
            mapParameters += (xx(0) -> xx(1))
            println(xx(0) + ": " + xx(1))
        }
        mapParameters
    }
    
    /**
     * vinhdp - 2016-06-09
     */
    
    def getListFilePathsByTiming(filePath: String, logDate: String, calculateValue: String): List[String] = {

        var lists = List[String]()

        calculateValue match {
            case Constants.Timing.A1 => {

                lists = s"""$filePath/$logDate""" :: lists
            }
            case Constants.Timing.A3 => {

                val listDate = DateTimeUtils.getListDateBefore(3, logDate)
                listDate.foreach {
                    date =>
                    {

                        lists = s"""$filePath/$date""" :: lists
                    }
                }
            }
            case Constants.Timing.A7 => {

                val listDate = DateTimeUtils.getListDateBefore(7, logDate)
                listDate.foreach {
                    date =>
                    {

                        lists = s"""$filePath/$date""" :: lists
                    }
                }
            }
            case Constants.Timing.A14 => {

                val listDate = DateTimeUtils.getListDateBefore(14, logDate)
                listDate.foreach {
                    date =>
                    {

                        lists = s"""$filePath/$date""" :: lists
                    }
                }
            }
            case Constants.Timing.A30 => {

                val listDate = DateTimeUtils.getListDateBefore(30, logDate)
                listDate.foreach {
                    date =>
                    {

                        lists = s"""$filePath/$date""" :: lists
                    }
                }
            }
            case Constants.Timing.A60 => {

                val listDate = DateTimeUtils.getListDateBefore(60, logDate)
                listDate.foreach {
                    date =>
                    {

                        lists = s"""$filePath/$date""" :: lists
                    }
                }
            }
            case Constants.Timing.A90 => {

                val listDate = DateTimeUtils.getListDateBefore(90, logDate)
                listDate.foreach {
                    date =>
                    {

                        lists = s"""$filePath/$date""" :: lists
                    }
                }
            }
            case Constants.Timing.A180 => {

                val listDate = DateTimeUtils.getListDateBefore(180, logDate)
                listDate.foreach {
                    date =>
                    {

                        lists = s"""$filePath/$date""" :: lists
                    }
                }
            }
        }

        lists
    }

    def getListFilePathsByKPI(filePath: String, logDate: String, calculateValue: String): List[String] = {

        var lists = List[String]()

        calculateValue match {
            case Constants.Timing.AC1 => {    // daily

                lists = s"""$filePath/$logDate""" :: lists
            }
            case Constants.Timing.AC7 => {    // weekly

                val listDate = DateTimeUtils.getWeeklyDate(logDate)

                listDate.foreach {
                    date =>
                    {

                        lists = s"""$filePath/$date""" :: lists
                    }
                }
            }
            case Constants.Timing.AC30 => {    // monthly

                val listDate = DateTimeUtils.getMonthlyDate(logDate)

                listDate.foreach {
                    date =>
                    {

                        lists = s"""$filePath/$date""" :: lists
                    }
                }
            }
            case Constants.Timing.AC60 => {    // 2 month

                val listDate = DateTimeUtils.get2MonthDate(logDate)

                listDate.foreach {
                    date =>
                    {

                        lists = s"""$filePath/$date""" :: lists
                    }
                }
            }
        }

        lists
    }
    
    def formatReportDate(logDate: String): String = {
        
        return logDate + " 00:00:00"
    }



    def isExist(lstFiles: String): Boolean = {
        
        val fs = FileSystem.get(new Configuration(true));
        var isExist = true;
        
        lstFiles.split(",").map {
            
            file => 
            
            val path = new Path(file)
            if (!fs.exists(path)) {
                 isExist = false   
            }
        }
        
        return isExist
    }
    
    def getFile(filePath: String, logDate: String, timing: String): String = {
        
        var date = DateTimeUtils.getDateBefore(logDate, timing)
        s"""$filePath/$date"""
    }
    
    def getFile(filePath: String, logDate: String): String = {
        
        s"""$filePath/$logDate"""
    }

    /******************************************** END *************************************************************/

    def getListFilePaths(filePath: String, logDate: String, calculateMethod: String, calculateValue: String): List[String] = {

        var lists = List[String]()

        calculateMethod match {
            case Constants.KPI => {

                lists = getListFilePathsByKPI(filePath, logDate, calculateValue)
            }

            case Constants.TIMING => {

                lists = getListFilePathsByTiming(filePath, logDate, calculateValue)
            }
        }

        lists
    }

    def getListFilePathsBetweenNTimes(filePath: String, logDate: String, calculateMethod: String, calculateValue: String, distance: Integer): List[String] = {

        var lists = List[String]()
        var newLogDate = DateTimeUtils.getDateDifferent(distance, logDate, calculateMethod, calculateValue)

        calculateMethod match {
            case Constants.KPI => {

                lists = getListFilePathsByKPI(filePath, newLogDate, calculateValue)
            }

            case Constants.TIMING => {

                lists = getListFilePathsByTiming(filePath, newLogDate, calculateValue)
            }
        }

        lists
    }

    

    def formatCode(code: String): String = {

        var newCode = "";
        newCode = code.replaceAll("_", "-").replaceAll(" ", "").toLowerCase()
        newCode
    }
    
    def isEmpty(file: String): Boolean = {

        val fs = FileSystem.get(new Configuration(true));
        var isEmpty = true;
        val path = new Path(file)
        if (fs.exists(path)) {
            
            isEmpty = false
        }

        isEmpty
    }
    
    def isParquetEmpty(lstFiles: List[String]): Boolean = {

        val fs = FileSystem.get(new Configuration(true));
        var isEmpty = true;

        breakable {
            for (i <- 0 to lstFiles.size - 1) {

                val path = new Path(lstFiles.apply(i) + "/_metadata")
                if (fs.exists(path)) {

                    isEmpty = false;
                    break;
                }
            }
        }

        isEmpty
    }

    def isEmpty(lstFiles: List[String]): Boolean = {

        val fs = FileSystem.get(new Configuration(true));
        var isEmpty = true;

        breakable {
            for (i <- 0 to lstFiles.size - 1) {

                val path = new Path(lstFiles.apply(i))
                if (fs.exists(path)) {

                    isEmpty = false;
                    break;
                }
            }
        }

        isEmpty
    }

    def isEmpty(lstFiles: List[String], fs: FileSystem): Boolean = {

        var isEmpty = true;

        breakable {
            for (i <- 0 to lstFiles.size - 1) {

                val path = new Path(lstFiles.apply(i))
                if (fs.exists(path)) {

                    isEmpty = false;
                    break;
                }
            }
        }

        isEmpty
    }



    // new function to get inputPath, for both daily and hourly
    def getIngameHourlyPathTmp(filePath: String, logDate: String, numberOfFiles: Integer, hour: String): String = {
        val sb = new StringBuilder
        sb.append(filePath + "/{" + logDate + "_" + hour)

        for (i <- 1 to numberOfFiles) {
            val date = DateTimeUtils.getDateDifferent(i * -1, logDate, Constants.TIMING, Constants.A1);
            sb.append(",")
            //sb.append(filePath)
            //sb.append("/")
            sb.append(date)
            sb.append("_23")
        }
        sb.append("}")

        sb.toString()
    }

    /**
     * Return number of file before log date, separate by comma
     */
    def getMultiFiles(filePath: String, logDate: String, numberOfFiles: Integer): String = {

        val sb = new StringBuilder
        sb.append(filePath + "/" + logDate)

        for (i <- 1 to numberOfFiles) {

            val date = DateTimeUtils.getDateDifferent(i * -1, logDate, Constants.TIMING, Constants.A1);

            sb.append(",")
            sb.append(filePath)
            sb.append("/")
            sb.append(date)
        }

        sb.toString()
    }
    
    def getMultiFileWithZone(filePath: String, logDate: String, numberOfFiles: Integer): String = {

        val sb = new StringBuilder
        sb.append(filePath + "/" + logDate)

        for (i <- 1 to numberOfFiles) {

            val date = DateTimeUtils.getDateDifferent(i * -1, logDate, Constants.TIMING, Constants.A1);

            sb.append(",")
            sb.append(filePath)
            sb.append("/")
            sb.append(date)
        }
        
        for (i <- 1 to numberOfFiles) {

            val date = DateTimeUtils.getDateDifferent(i * 1, logDate, Constants.TIMING, Constants.A1);

            sb.append(",")
            sb.append(filePath)
            sb.append("/")
            sb.append(date)
        }

        sb.toString()
    }

    def getMultiSdkLogFiles(filePath: String, logDate: String, fileName: String, numberOfFiles: Integer): String = {
        var rs = ""
        rs += filePath + "/{" + logDate + "/" + fileName + "/" + fileName + "-" + logDate + ".gz,"
        for (i <- 1 to numberOfFiles) {
            val date = DateTimeUtils.getDateDifferent(i * -1, logDate, Constants.TIMING, Constants.A1)
            rs += "/" + date + "/" + fileName + "/" + fileName + "-" + date + ".gz"
            rs += ","
        }
        rs=rs.dropRight(1)
        rs += "}"
        rs
    }
    def getMultiSdkLogFilesHourly(filePath: String, logDate: String, fileName: String, numberOfFiles: Integer): String = {
        var rs = ""
        //rs += filePath + "/{" + logDate + "/" + fileName + "/" + fileName + "-" + logDate + "_000*.gz,"
        rs += filePath + "/{" + logDate + "/" + fileName + "/" + fileName + "-" + logDate + "_000*,"
        for (i <- 1 to numberOfFiles) {
            val date = DateTimeUtils.getDateDifferent(i * -1, logDate, Constants.TIMING, Constants.A1)
            //rs += "/" + date + "/" + fileName + "/" + fileName + "-" + date + "_000*.gz"
            rs += "/" + date + "/" + fileName + "/" + fileName + "-" + date + "_000*"
            rs += ","
        }
        rs=rs.dropRight(1)
        rs += "}"
        rs
    }
    
    def getMultiSdkThaiLogFiles(filePath: String, logDate: String, fileName: String, numberOfFiles: Integer): String = {
        var rs = ""
        rs += filePath + "{/Web_" + logDate + "/" + fileName + "-" + logDate + ".gz"
        for (i <- 1 to numberOfFiles) {
            rs += ","
            val date = DateTimeUtils.getDateDifferent(i * -1, logDate, Constants.TIMING, Constants.A1)
            rs += "/Web_" + date + "/" + fileName + "-" + date + ".gz"
            rs += ","
        }
        rs=rs.dropRight(1)
        rs += "}"
        rs
    }

    def getOneFileHourly(filePath: String, fileName: String): String = {
        val rs = filePath  + "/" + fileName + "/" + fileName + "*"
        rs
    }

    def getListParquetFilesByTimming(timming: String, log_date: String, path: String, parquet_folder: String): List[String] = {
        var listFilePaths = List[String]()

        timming match {
            case Constants.A1 => {
                listFilePaths = path + "/" + parquet_folder + "/" + log_date :: listFilePaths
            }
            case Constants.A7 => {
                val listDate = DateTimeUtils.getListDateBefore(7, log_date)
                listDate.foreach {
                    date =>
                    {
                        listFilePaths = path + "/" + parquet_folder + "/" + date :: listFilePaths
                    }
                }
            }
            case Constants.A30 => {

                val listDate = DateTimeUtils.getListDateBefore(30, log_date)
                listDate.foreach {
                    date =>
                    {
                        listFilePaths = path + "/" + parquet_folder + "/" + date :: listFilePaths
                    }
                }
            }
            case Constants.MONTHLY => {
                val listDate = DateTimeUtils.getMonthlyDate(log_date)
                listDate.foreach {
                    date =>
                    {
                        listFilePaths = path + "/" + parquet_folder + "/" + date :: listFilePaths
                    }
                }
            }

        }

        listFilePaths
    }

    /**
     * get list of csv files (files by log team)
     *
     */
    def getListRawFilesByTimming(timming: String, log_date: String, path: String): List[String] = {
        var listFilePaths = List[String]()

        timming match {
            case Constants.A1 => {
                listFilePaths = path + "/" + log_date + "/part*" :: listFilePaths
            }
            case Constants.A7 => {
                val listDate = DateTimeUtils.getListDateBefore(7, log_date)
                listDate.foreach {
                    date =>
                    {
                        listFilePaths = path + "/" + date + "/part*" :: listFilePaths
                    }
                }
            }
            case Constants.A30 => {

                val listDate = DateTimeUtils.getListDateBefore(30, log_date)
                listDate.foreach {
                    date =>
                    {
                        listFilePaths = path + "/" + date + "/part*" :: listFilePaths
                    }
                }
            }
            case Constants.MONTHLY => {
                val listDate = DateTimeUtils.getMonthlyDate(log_date)
                listDate.foreach {
                    date =>
                    {
                        listFilePaths = path + "/" + date + "/part*" :: listFilePaths
                    }
                }
            }
        }

        listFilePaths
    }

    def remNotExists(files:String): String = {
        val sb = new StringBuilder
        val lstFiles = files.split(",")
        val fs = FileSystem.get(new Configuration(true))
        var rs=true
        for (i <- 0 to lstFiles.size - 1) {

            val path = new Path(lstFiles.apply(i))
            if (fs.exists(path)) {
                if(rs){
                    sb.append(lstFiles(i))
                    rs = false
                }else{
                    sb.append(",")
                    sb.append(lstFiles(i))
                }
            }
        }
        sb.toString()
    }

    def main(args: Array[String]) {

        var lst = getListFiles("/ge/warehouse/bklr/ub/data/payment_2", "2016-08-13", "ac7")
        lst.foreach { x => println(x) }
    }
}