package vng.stats.ub.report3.group

import vng.stats.ub.sql.report.MysqlGroupReport
import vng.stats.ub.common.KpiGroupFormat
import org.apache.spark.sql.DataFrame
import vng.stats.ub.report3.BaseReport
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import org.apache.spark.sql.functions._

object GroupNewUserRevenueReport extends BaseReport {

    def excute(dataframe: DataFrame): DataFrame = {
        
        var resultDF = dataframe.select(groupId, calcId, "net_amt", "gross_amt")
        .withColumn(groupId, makeOtherIfNull(dataframe(groupId)))
        .groupBy(groupId).agg(countDistinct(calcId), sum("net_amt"), sum("gross_amt"))
        
        resultDF
    }
    
    def store(dataframe: DataFrame): Unit = {
        
        var output = List[KpiGroupFormat]()
        
        dataframe.collect().foreach { row =>
            var groupId = row.getString(0)
            var totalPaying = row.getLong(1)
            var totalNetRevenue = row.getDouble(2)
            var totalGrossRevenue = row.getDouble(3)

            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_USER_PAYING, timing), totalPaying) :: output
            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_USER_PAYING_NET_REVENUE, timing), totalNetRevenue) :: output
            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_USER_PAYING_GROSS_REVENUE, timing), totalGrossRevenue) :: output
            
            Common.logger("groupId: " + groupId + ", new user paying: " + totalPaying + ", net revenue: " + totalNetRevenue + ", gross revenue: " + totalGrossRevenue)
        }
        
        if(groupId == "sid" || groupId == "country" || groupId == "os"){
            
            MysqlGroupReport.insertJson(output, groupId, calcId)
        } else {
            MysqlGroupReport.insert(output, groupId, calcId)
        }
    }
}