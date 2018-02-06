package vng.stats.ub.report2.game

import scala.reflect.runtime.universe
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import vng.stats.ub.common.KpiFormat
import vng.stats.ub.report2.BaseReport
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.sql.report.MysqlGameReport

object KpiActiveUser extends BaseReport {

    var activityPath = ""
    
    override def readExtraParams(): Unit = {
        
        activityPath = parameters(Constants.Parameters.ACTIVITY_PATH)
    }
    
    def excute(sqlContext: SQLContext): DataFrame = {
        
        var lstLogFiles = DataUtils.getListFiles(activityPath, logDate, timing);
        var logDF = sqlContext.read.option("mergeSchema", "true").parquet(lstLogFiles: _*)

        var dUserDF = logDF.select(calcId).distinct()
        var active = dUserDF.count
        
        var output = List[KpiFormat]()
        var id = DataUtils.getKpiId(calcId, Constants.Kpi.ACTIVE, timing)
        output = KpiFormat(source, gameCode, reportDate, createDate, id, active) :: output
        
        Common.logger("Total: " + active + ", id: " + id)
        
        MysqlGameReport.insert(output, calcId)
        
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        
        df
    }
}