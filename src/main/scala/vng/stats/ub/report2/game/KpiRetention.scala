package vng.stats.ub.report2.game

import scala.reflect.runtime.universe
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import vng.stats.ub.common.KpiFormat
import vng.stats.ub.common.Schemas
import vng.stats.ub.report2.BaseReport
import vng.stats.ub.utils.Common
import vng.stats.ub.utils.Constants
import vng.stats.ub.utils.DataUtils
import vng.stats.ub.sql.report.MysqlGameReport

object KpiRetention extends BaseReport {

    var activityPath = ""
    var accregisterPath = ""
    
    override def readExtraParams(): Unit = {
        
        accregisterPath = parameters(Constants.Parameters.ACC_REGISTER_PATH)
        activityPath = parameters(Constants.Parameters.ACTIVITY_PATH)
    }

    def excute(sqlContext: SQLContext): DataFrame = {
        
        var loginFile = DataUtils.getFile(activityPath, logDate);
        var accRegisterFile = DataUtils.getFile(accregisterPath, logDate, timing);

        var userDF = sqlContext.read.option("mergeSchema", "true").parquet(loginFile).select(calcId).distinct()
        var newAccDF = sqlContext.createDataFrame(sc.emptyRDD[Row], Schemas.AccountRegister)
        var rate = 0.0
        var retention = 0L
        var newAcc = 0L
        var output = List[KpiFormat]()
        
        if (!DataUtils.isEmpty(accRegisterFile)) {

            newAccDF = sqlContext.read.option("mergeSchema", "true").parquet(accRegisterFile)
            var joinDF = newAccDF.join(userDF, newAccDF(calcId) === userDF(calcId))
            retention = joinDF.count
            newAcc = newAccDF.count

            Common.logger("Total: " + newAcc)
            Common.logger("Retention: " + retention)

            if (newAcc != 0) {
                rate = retention * 100.0 / newAcc
            }
        }
        
        output = KpiFormat(source, gameCode, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.NEW_USER_RETENTION_RATE, timing), rate) :: output
        Common.logger("Game: " + gameCode + ", new: " + newAcc + ", retention: " + retention)
        
        MysqlGameReport.insert(output, calcId)
        
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        
        df
    }
}