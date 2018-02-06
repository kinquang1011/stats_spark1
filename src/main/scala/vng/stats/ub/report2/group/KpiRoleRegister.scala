package vng.stats.ub.report2.group

import vng.stats.ub.utils.DataUtils
import vng.stats.ub.report2.BaseReport
import org.apache.spark.sql.DataFrame
import vng.stats.ub.utils.Common
import org.apache.spark.sql.SQLContext
import vng.stats.ub.common.KpiFormat
import vng.stats.ub.utils.Constants
import org.apache.spark.sql.functions._
import vng.stats.ub.common.KpiGroupFormat
import vng.stats.ub.sql.report.MysqlGroupReport

object KpiRoleRegister extends BaseReport {

    var roleRegisterPath = ""
    
    override def readExtraParams(): Unit = {
        
        roleRegisterPath = parameters(Constants.Parameters.ROLE_REGISTER_PATH)
    }
    
    def excute(sqlContext: SQLContext): DataFrame = {
        
        var lstLogFiles = DataUtils.getListFiles(roleRegisterPath, logDate, timing);
        var logDF = sqlContext.read.parquet(lstLogFiles: _*)

        var roleDF = logDF.select(groupId, calcId).distinct.groupBy(groupId).agg(count(calcId))
        
        var output = List[KpiGroupFormat]()
        
        roleDF.collect().foreach { row =>
            var groupId = row.getString(0)
            var newRole = row.getLong(1)

            output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_ROLE_PLAYING, timing), newRole) :: output
            
            Common.logger("Group: " + groupId + ", new rolev: " + newRole)
        }
        
        MysqlGroupReport.insert(output, groupId, calcId)
        
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        
        df
    }
}