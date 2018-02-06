package vng.stats.ub.report2.group

import scala.reflect.runtime.universe
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.sum
import vng.stats.ub.common.KpiGroupFormat
import vng.stats.ub.report2.BaseReport
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.sql.report.MysqlGroupReport

object KpiNewUserPaying extends BaseReport {
    
    var paymentPath = ""
    var accregisterPath = ""
    
    override def readExtraParams(): Unit = {
        
        paymentPath = parameters(Constants.Parameters.PAYMENT_PATH)
        accregisterPath = parameters(Constants.Parameters.ACC_REGISTER_PATH)   
    }
    
    def excute(sqlContext: SQLContext): DataFrame = {

        var lstPaymentFiles = DataUtils.getListFiles(paymentPath, logDate, timing)
        var lstNewAccFiles = DataUtils.getListFiles(accregisterPath, logDate, timing)
        
        var paymentDF = sqlContext.read.parquet(lstPaymentFiles: _*)
        var newAccDF = sqlContext.read.parquet(lstNewAccFiles: _*)
        
        //var joinDF = paymentDF.join(newAccDF, paymentDF(calcId) === newAccDF(calcId) && paymentDF(groupId) === newAccDF(groupId), "leftsemi")
        /**
         * Modify date: 2016-08-10
         * By: vinhdp
         * Modify: remove join by groupId
         */
        var joinDF = paymentDF.join(newAccDF, paymentDF(calcId) === newAccDF(calcId), "leftsemi")
        
        var revenueDF = joinDF.select(groupId, calcId, "net_amt").groupBy(groupId).agg(countDistinct(calcId), sum("net_amt"))
        var output = List[KpiGroupFormat]()
        
        revenueDF.collect().foreach { row =>
            var groupId = row.getString(0)
            var totalPaying = row.getLong(1)
            var totalRevenue = row.getDouble(2)

            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_USER_PAYING, timing), totalPaying) :: output
            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_USER_PAYING_NET_REVENUE, timing), totalRevenue) :: output
            
            Common.logger("groupId: " + groupId + ", new user paying: " + totalPaying + ", revenue: " + totalRevenue)
        }
        
        MysqlGroupReport.insert(output, groupId, calcId)
        
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        
        df
    }
}