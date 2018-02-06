package vng.stats.ub.report2.group

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
import org.apache.spark.sql.functions.countDistinct
import vng.stats.ub.common.KpiGroupFormat
import vng.stats.ub.sql.report.MysqlGroupReport

object KpiFirstChargeRetention extends BaseReport {

    var firstChargePath = ""
    var paymentPath = ""

    override def readExtraParams(): Unit = {

        paymentPath = parameters(Constants.Parameters.PAYMENT_PATH)
        firstChargePath = parameters(Constants.Parameters.FIRSTCHARGE_PATH)
    }

    def excute(sqlContext: SQLContext): DataFrame = {
        
        var paymentFile = DataUtils.getFile(paymentPath, logDate);
        var firstChargeFile = DataUtils.getFile(firstChargePath, logDate, timing);

        var paymentDF = sqlContext.read.parquet(paymentFile).select("game_code", groupId, calcId).distinct()
        var firstDF = sqlContext.createDataFrame(sc.emptyRDD[Row], Schemas.FirstCharge)
        var output = List[KpiGroupFormat]()
        
        if (!DataUtils.isEmpty(firstChargeFile)) {

            firstDF = sqlContext.read.parquet(firstChargeFile)
            var joinDF = firstDF.as('f).join(paymentDF.as('p),
                    firstDF("game_code") === paymentDF("game_code") && firstDF(calcId) === paymentDF(calcId) && firstDF(groupId) === paymentDF(groupId),
                    "left_outer")
            var resultDF = joinDF.groupBy("f.game_code", "f." + groupId).agg(countDistinct("f." + calcId), countDistinct("p." + calcId))
            
            resultDF.collect().foreach { row =>
                
                var rate = 0.0
                var gameCode = row.getString(0)
                var groupId = row.getString(1)
                var totalFirst = row.getLong(2)
                var totalRetention = row.getLong(3)
    
                if (totalFirst != 0) {
                    rate = totalRetention * 100.0 / totalFirst
                }
                output = KpiGroupFormat(source, gameCode, groupId, reportDate, createDate, DataUtils.getKpiId(calcId, Constants.Kpi.RETENTION_PAYING_RATE, timing), rate) :: output
                Common.logger("Game: " + gameCode + ", groupId: " + groupId +  ", first charge: " + totalFirst + ", retention: " + totalRetention)
            }
        }
        
        MysqlGroupReport.insert(output, groupId, calcId)
        
        var x = sc.parallelize(output, 1)
        var df = sqlContext.createDataFrame(x)
        
        df
    }
}