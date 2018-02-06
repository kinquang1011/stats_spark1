package vng.stats.ub.report3.group

import vng.stats.ub.sql.report.MysqlGroupReport
import vng.stats.ub.common.KpiGroupFormat
import org.apache.spark.sql.DataFrame
import vng.stats.ub.report3.BaseReport
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import org.apache.spark.sql.functions._

object GroupActiveReport extends BaseReport {

    def excute(dataframe: DataFrame): DataFrame = {
        
        var resultDF = dataframe.select(groupId, calcId)
        .withColumn(groupId, makeOtherIfNull(dataframe(groupId)))
        .distinct().groupBy(groupId).agg(count(calcId))
        resultDF
    }
    
    def store(dataframe: DataFrame): Unit = {
        
        var output = List[KpiGroupFormat]()
        
        dataframe.collect().foreach { row =>
            
            var groupId = row.getString(0)
            var totalActive = row.getLong(1)

            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.ACTIVE, timing), totalActive) :: output
            
            Common.logger("groupId: " + groupId + ", active: " + totalActive)
        }
        
        if(groupId == "sid" || groupId == "country" || groupId == "os"){
            
            MysqlGroupReport.insertJson(output, groupId, calcId)
        } else {
            MysqlGroupReport.insert(output, groupId, calcId)
        }
    }
}