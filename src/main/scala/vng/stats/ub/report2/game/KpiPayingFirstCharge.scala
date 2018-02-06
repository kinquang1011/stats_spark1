package vng.stats.ub.report2.game

import scala.reflect.runtime.universe
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.sum
import vng.stats.ub.common.KpiFormat
import vng.stats.ub.report2.BaseReport
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils
import org.apache.spark.sql.SQLContext
import vng.stats.ub.sql.report.MysqlGameReport

object KpiPayingFirstCharge extends BaseReport {
    
    var paymentPath = ""
    var firstChargePath = ""
    
    override def readExtraParams(): Unit = {
        
        paymentPath = parameters(Constants.Parameters.PAYMENT_PATH)
        firstChargePath = parameters(Constants.Parameters.FIRSTCHARGE_PATH)
    }
    
    def excute(sqlContext: SQLContext): DataFrame = {

        var lstPaymentFiles = DataUtils.getListFiles(paymentPath, logDate, timing)
        var lstFirstChargeFiles = DataUtils.getListFiles(firstChargePath, logDate, timing)
        
        var paymentDF = sqlContext.read.option("mergeSchema", "true").parquet(lstPaymentFiles: _*)
        var firstChargeDF = sqlContext.read.option("mergeSchema", "true").parquet(lstFirstChargeFiles: _*)
        
        var joinDF = paymentDF.as('p).join(firstChargeDF.as('f), 
                paymentDF("game_code") === firstChargeDF("game_code") && paymentDF(calcId) === firstChargeDF(calcId), "leftsemi")

        var revenueDF = joinDF.select("game_code", calcId, "net_amt", "gross_amt").coalesce(1).groupBy("game_code").agg(countDistinct(calcId), sum("net_amt"), sum("gross_amt"))
        var output = List[KpiFormat]()
        
        revenueDF.collect().foreach { row =>
            var gameCode = row.getString(0)
            var totalPaying = row.getLong(1)
            var totalNetRevenue = row.getDouble(2)
            var totalGrossRevenue = row.getDouble(3)

            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_PAYING, timing), totalPaying) :: output
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_PAYING_NET_REVENUE, timing), totalNetRevenue) :: output
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_PAYING_GROSS_REVENUE, timing), totalGrossRevenue) :: output
            
            Common.logger("Game: " + gameCode + ", first charge: " + totalPaying + ", net revenue: " + totalNetRevenue + ", gross revenue: " + totalGrossRevenue)
        }
        
        if (output.size == 0) {
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_PAYING, timing), 0) :: output
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_PAYING_NET_REVENUE, timing), 0) :: output
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_PAYING_GROSS_REVENUE, timing), 0) :: output
        }
        
        MysqlGameReport.insert(output, calcId)
        
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        
        df
    }
}