package vng.stats.ub.report3.group

import vng.stats.ub.sql.report.MysqlGroupReport
import vng.stats.ub.common.KpiGroupFormat
import org.apache.spark.sql.DataFrame
import vng.stats.ub.report3.BaseReport
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import org.apache.spark.sql.functions._

object GroupAccRegisterReport extends BaseReport {

    def excute(dataframe: DataFrame): DataFrame = {
        
        var resultDF = dataframe.select(groupId, calcId)
        .withColumn(groupId, makeOtherIfNull(dataframe(groupId)))
        .distinct.groupBy(groupId).agg(count(calcId))
        
        resultDF
    }
    
    def store(dataframe: DataFrame): Unit = {
        
        var output = List[KpiGroupFormat]()
        
        dataframe.collect().foreach { row =>
            var groupId = row.getString(0)
            var totalReg = row.getLong(1)

            var id = DataUtils.getKpiId(calcId, Constants.Kpi.NEW_ACCOUNT_PLAYING, timing)
            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, id, totalReg) :: output
            
            Common.logger("groupId: " + groupId + ", Total: " + totalReg + ", id: " + id)
        }
        
        if(groupId == "sid" || groupId == "country" || groupId == "os"){
            
            MysqlGroupReport.insertJson(output, groupId, calcId)
        } else {
            MysqlGroupReport.insert(output, groupId, calcId)
        }
    }

}