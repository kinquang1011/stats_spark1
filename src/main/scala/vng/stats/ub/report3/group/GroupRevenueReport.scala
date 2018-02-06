package vng.stats.ub.report3.group

import vng.stats.ub.report3.BaseReport
import vng.stats.ub.sql.report.MysqlGroupReport
import vng.stats.ub.common.KpiGroupFormat
import vng.stats.ub.utils.DataUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants

object GroupRevenueReport extends BaseReport {

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

            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.PAYING_USER, timing), totalPaying) :: output
            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NET_REVENUE, timing), totalNetRevenue) :: output
            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.GROSS_REVENUE, timing), totalGrossRevenue) :: output
            
            Common.logger("groupId: " + groupId + ", paying: " + totalPaying + ", net revenue: " + totalNetRevenue + ", gross revenue: " + totalGrossRevenue)
        }
        
        if(groupId == "sid" || groupId == "country" || groupId == "os"){
            
            MysqlGroupReport.insertJson(output, groupId, calcId)
        } else {
            MysqlGroupReport.insert(output, groupId, calcId)
        }
    }
}