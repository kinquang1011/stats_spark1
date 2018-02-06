package vng.stats.ub.report3.calc

import vng.stats.ub.sql.report.MysqlGroupReport
import vng.stats.ub.common.KpiGroupFormat
import org.apache.spark.sql.DataFrame
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.sum
import vng.stats.ub.report3.BaseReport

trait GroupRevenueCalc extends BaseReport{

    def excute(dataframe: DataFrame): DataFrame = {

        var revenueDF = dataframe.groupBy(groupId).agg(countDistinct(calcId), sum("net_amt"))
        revenueDF
    }
    
    def store(dataframe: DataFrame): Unit = {
        
        var output = List[KpiGroupFormat]()
        
        dataframe.collect().foreach { row =>
            var groupId = row.getString(0)
            var totalPaying = row.getLong(1)
            var totalRevenue = row.getDouble(2)

            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.PAYING_USER, timing), totalPaying) :: output
            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NET_REVENUE, timing), totalRevenue) :: output
            
            Common.logger("groupId: " + groupId + ", paying: " + totalPaying + ", revenue: " + totalRevenue)
        }
        
        //MysqlGroupReport.insert(output, groupId)
    }
}