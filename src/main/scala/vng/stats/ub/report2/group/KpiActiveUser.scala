package vng.stats.ub.report2.group

import vng.stats.ub.report2.BaseReport
import vng.stats.ub.common.KpiFormat
import vng.stats.ub.utils.DataUtils
import org.apache.spark.sql.functions._
import vng.stats.ub.common.KpiGroupFormat
import java.util.Date
import vng.stats.ub.utils.DateTimeUtils
import vng.stats.ub.utils.Constants
import vng.stats.ub.common.Schemas
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import vng.stats.ub.utils.Common
import org.apache.spark.sql.SQLContext
import vng.stats.ub.sql.report.MysqlGroupReport

object KpiActiveUser extends BaseReport {

    var activityPath = ""
    
    override def readExtraParams(): Unit = {
        
        activityPath = parameters(Constants.Parameters.ACTIVITY_PATH)
    }
    
    def excute(sqlContext: SQLContext): DataFrame = {

        var lstLoginPaths = DataUtils.getListFiles(activityPath, logDate, timing);
        var loginDF = sqlContext.read.parquet(lstLoginPaths: _*)

        var userDF = loginDF.select(groupId, calcId).distinct()
        var activeDF = userDF.groupBy(groupId).agg(count(calcId))
        var output = List[KpiGroupFormat]()
        
        activeDF.collect().foreach { row =>
            var groupId = row.getString(0)
            var totalActive = row.getLong(1)

            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.ACTIVE, timing), totalActive) :: output
            
            Common.logger("groupId: " + groupId + ", active: " + totalActive)
        }
        
        MysqlGroupReport.insert(output, groupId, calcId)
        
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        
        df
    }
}