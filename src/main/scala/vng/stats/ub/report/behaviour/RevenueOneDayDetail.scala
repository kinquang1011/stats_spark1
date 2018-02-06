package vng.stats.ub.report.behaviour

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.{Locale, Calendar}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import vng.stats.ub.db.MysqlDB
import vng.stats.ub.utils.{DateTimeUtils, Common}
import com.google.gson.Gson
/**
 * Created by tuonglv on 24/06/2016.
 */

//case class RODDDataObject(data:String, interval: Long)
/*
object RevenueOneDayDetail {
    def main(args: Array[String]) {
        var mapParameters: Map[String, String] = Map()
        for (x <- args) {
            var xx = x.split("=")
            mapParameters += (xx(0) -> xx(1))
        }

        val logDate = mapParameters("logDate")
        val gameCode = mapParameters("gameCode")
        var isSdk = false
        val minutesPlus = 60

        if (mapParameters("sdkLog") == "true") {
            isSdk = true
        }

        val conf = new SparkConf().setAppName("Kpi Paying Report")
        conf.set("spark.hadoop.validateOutputSpecs", "false")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

        val path = Common.getOuputParquetPath(gameCode, "payment", logDate, isSdk)
        //val path = "/ge/warehouse/nikki/ub/sdk_data/payment/2016-06-21"
        val paymentDF = sqlContext.read.parquet(path)
        //val groupByTime = paymentDF.selectExpr("CONCAT(substring(date_format(log_date,\"Y-MM-dd HH:m\"),0,15),0) as f_log_date","gross_amt").groupBy("f_log_date").agg(sum("gross_amt")).orderBy("f_log_date")
        val groupByTime = paymentDF.selectExpr("log_date as f_log_date", "gross_amt")
        groupByTime.cache

        val inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date = inputFormat.parse(logDate + " 00:00:00")
        val calendar = Calendar.getInstance(Locale.UK)
        calendar.setTime(date)
        var times = (24 * 60) / minutesPlus
        if ((24 * 60) % minutesPlus != 0) {
            times = times + 1
        }
        var beginDate = logDate + " 00:00:00"
        val dataA: Array[Double] = new Array[Double](times)
        for (i <- 1 to times) {
            calendar.add(Calendar.MINUTE, minutesPlus)
            val endDate = outputFormat.format(calendar.getTime)
            val sumDF = groupByTime.coalesce(1).where("f_log_date >= '" + beginDate + "' and f_log_date <'" + endDate + "'").agg(sum("gross_amt"))
            if (sumDF.first == Row(null)) {
                dataA(i - 1) = 0
            } else {
                dataA(i - 1) = sumDF.first.getDouble(0)
            }
            beginDate = endDate
        }
        val p = RODDDataObject(dataA.mkString(","), minutesPlus)
        val gson = new Gson
        val jsonString = gson.toJson(p)

        val logType = "dgr1"
        val dataSource = "sdk"
        val calcDate = DateTimeUtils.getCurrentDatetime()
        val dataString = gameCode + "\t" + logDate + "\t" + dataSource + "\t" + logType + "\t" + jsonString + "\t" + calcDate
        val insertSql = "insert into game_behavior(game_code, report_date, source, report_id, report_value, calc_date) " +
            "values('" + gameCode + "', '" + logDate + "', '" + dataSource + "','" + logType + "','" + jsonString + "','" + calcDate + "')"
        val mysqlDB = MysqlDB
        Common.logger(insertSql)
        mysqlDB.executeQuery(insertSql)
        groupByTime.unpersist()
        val outputPath = Common.getBehaviourReportPath(gameCode, "", logDate, isSdk)
        //val rdd = sc.parallelize(Seq(dataString))
        //rdd.coalesce(1).saveAsTextFile(outputPath)
    }
}
*/