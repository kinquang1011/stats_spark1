package vng.stats.ub.report2.game

import scala.reflect.runtime.universe
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.sum
import vng.stats.ub.common.KpiFormat
import vng.stats.ub.report2.BaseReport
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.sql.report.MysqlGameReport

object KpiNewUserPaying extends BaseReport {

    var paymentPath = ""
    var accregisterPath = ""
    
    override def readExtraParams(): Unit = {
        
        paymentPath = parameters(Constants.Parameters.PAYMENT_PATH)
        accregisterPath = parameters(Constants.Parameters.ACC_REGISTER_PATH)   
    }

    def excute(sqlContext: SQLContext): DataFrame = {

        var lstPaymentFiles = DataUtils.getListFiles(paymentPath, logDate, timing);
        var paymentDF = sqlContext.read.option("mergeSchema", "true").parquet(lstPaymentFiles: _*).select("game_code", calcId, "net_amt", "gross_amt")
        var lstNewAccFiles = DataUtils.getListFiles(accregisterPath, logDate, timing);

        var firstDF = sqlContext.read.option("mergeSchema", "true").parquet(lstNewAccFiles: _*)

        var resultDF = paymentDF.as('e).join(firstDF.as('f),
            paymentDF("game_code") === firstDF("game_code") && paymentDF(calcId) === firstDF(calcId)).
            groupBy("e.game_code").agg(sum("e.net_amt").alias("first_revenue"), sum("e.gross_amt").alias("gross_revenue"), countDistinct("e." + calcId).alias("first_user")).coalesce(1)

        var output = List[KpiFormat]()

        resultDF.collect().foreach { row =>
            var gameCode = row.getString(0)
            var newpayingNetRevenue = row.getDouble(1)
            var newpayingGrossRevenue = row.getDouble(2)
            var newpaying = row.getLong(3)

            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_USER_PAYING, timing), newpaying) :: output
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_USER_PAYING_NET_REVENUE, timing), newpayingNetRevenue) :: output
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_USER_PAYING_GROSS_REVENUE, timing), newpayingGrossRevenue) :: output

            Nil
        }

        if (output.size == 0) {
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_USER_PAYING, timing), 0) :: output
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_USER_PAYING_NET_REVENUE, timing), 0) :: output
            output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_USER_PAYING_GROSS_REVENUE, timing), 0) :: output
        }
        
        MysqlGameReport.insert(output, calcId)
        
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        
        df
    }
}