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

object KpiAccountRegister extends BaseReport {

    var accregisterPath = ""
    
    override def readExtraParams(): Unit = {
        
        accregisterPath = parameters(Constants.Parameters.ACC_REGISTER_PATH)
    }
    
    def excute(sqlContext: SQLContext): DataFrame = {
        
        var accRegisterDF = sqlContext.createDataFrame(sc.emptyRDD[Row], Schemas.AccountRegister)
        var accRegister = 0L

        var lstRegisterPath = DataUtils.getListFiles(accregisterPath, logDate, timing);

        if (!DataUtils.isEmpty(lstRegisterPath)) {

            accRegisterDF = sqlContext.read.option("mergeSchema", "true").parquet(lstRegisterPath: _*)
            accRegister = accRegisterDF.select(calcId).distinct().count()
        }
        
        var output = List[KpiFormat]()
        var id = DataUtils.getKpiId(calcId, Constants.Kpi.NEW_ACCOUNT_PLAYING, timing)
        output = KpiFormat(source, gameCode, reportDate, createDate, id, accRegister) :: output
        
        Common.logger("Total: " + accRegister + ", id: " + id)
        
        MysqlGameReport.insert(output, calcId)
        
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        
        df
    }
}