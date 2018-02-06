package vng.stats.ub.report2.game

import scala.reflect.runtime.universe
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.sum
import vng.stats.ub.common.KpiFormat
import vng.stats.ub.report2.BaseReport
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.sql.report.MysqlGameReport

object KpiPaying extends BaseReport {

    var paymentPath = ""

    override def readExtraParams(): Unit = {

        paymentPath = parameters(Constants.Parameters.PAYMENT_PATH)
    }
    
    def excute(sqlContext: SQLContext): DataFrame = {

        var lstLogFiles = DataUtils.getListFiles(paymentPath, logDate, timing);
        var paymentDF = sqlContext.read.option("mergeSchema", "true").parquet(lstLogFiles: _*).select("game_code", calcId, "net_amt", "gross_amt")

        var resultDF = paymentDF.filter("game_code is not null").groupBy("game_code").agg(sum("net_amt").alias("net_revenue"), sum("gross_amt").alias("gross_revenue"), countDistinct(calcId).alias("paying_user"))

        var output = List[KpiFormat]()
        
        resultDF.collect().foreach { row =>
            var gameCode = row.getString(0)
            var netRevenue = row.getDouble(1)
            var grossRevenue = row.getDouble(2)
            var pu = row.getLong(3)

            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.PAYING_USER, timing), pu) :: output
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NET_REVENUE, timing), netRevenue) :: output
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.GROSS_REVENUE, timing), grossRevenue) :: output
            
            Common.logger("Game: " + gameCode + ", Total: " + pu + ", revenue: " + netRevenue)
        }
        
        if (output.size == 0) {
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.PAYING_USER, timing), 0) :: output
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NET_REVENUE, timing), 0) :: output
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.GROSS_REVENUE, timing), 0) :: output
        }

        MysqlGameReport.insert(output, calcId)
        
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)

        df
    }
}