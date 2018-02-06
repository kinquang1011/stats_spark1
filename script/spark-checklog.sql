Revenue Calc from UB parquet:
*****************************
Range Days
var df = sqlContext.read.parquet("/ge/warehouse/tlbbm/ub/data/payment_2/2015-06-26").selectExpr("cast(net_amt as long) as amt").agg(sum("amt")).show

One Day
var df = sqlContext.read.parquet("/ge/warehouse/tlbbm/ub/data/payment_2/2015-06-2*").selectExpr("substring(log_date,0,10) as log_date","cast(net_amt as long) as amt").groupBy("log_date").agg(sum("amt")).show

=====================================================================================================================================================================================================
=====================================================================================================================================================================================================
Active Calc from UB parquet:
*****************************

Range Days:
var df = sqlContext.read.parquet("/ge/warehouse/tlbbm/ub/data/activity_2/2015-06-*").selectExpr("substring(log_date,0,10) as log_date","id").groupBy("log_date").agg(countDistinct("id")).show

One Day
var df = sqlContext.read.parquet("/ge/warehouse/tlbbm/ub/data/activity_2/2015-06-26").select("id").distinct.count

/****************************************************************************************************************************************************************************************************
*
*****************************************************************************************************************************************************************************************************/
Revenue Calc from raw log:
**************************

STONYVN:
val df = sc.textFile("/ge/warehouse/stonyvn/recharge/2016-09-*").map(_.split("\t")).map(row => (row(12).toLong, row(8))).toDF.selectExpr("substring(_2,0,10) as log_date","_1").groupBy("log_date").agg(sum("_1")).show

CONTRA:
val df = sc.textFile("/ge/warehouse/contra/money_flow/2015-10-1*").map(_.split("\t")).map(row => (if(row(10) == "1" && row(12) == "0") row(9).toLong * 200 else 0L, row(2))).toDF.selectExpr("substring(_2,0,10) as log_date","_1").groupBy("log_date").agg(sum("_1")).show

TLBBM:
val df = sc.textFile("/ge/warehouse/tlbbm/recharge/2016-08-27").map(_.split("\t")).map(row => (if(row(13)=="3000" && row(10) < "50") 500000L else row(13).toLong *200, row(0))).toDF.selectExpr("substring(_2,0,10) as log_date","_1").groupBy("log_date").agg(sum("_1")).show

SKY GARDEN GLOBAL:
val df = sc.textFile("/ge/warehouse/cgmbgfbs1/payment/2016-10-0*").map(_.split("\t")).map(row => (if(row(15)=="0") row(7).toLong else 0L, row(0), row(4))).toDF.selectExpr("substring(_2,0,10) as log_date","_1").groupBy("log_date").agg(sum("_1")).show

val df = sc.textFile("/ge/warehouse/cgmbgfbs1/payment/2016-10-0*").map(_.split("\t")).map(row => (if(row(15)=="0") row(7).toLong else 0L, row(0), row(4))).toDF.dropDuplicates(Seq("_3").selectExpr("substring(_2,0,10) as log_date","_1").groupBy("log_date").agg(sum("_1")).show
.sort("log_date", fieldDistinct(0)).dropDuplicates(Seq(fieldDistinct(0)))
=====================================================================================================================================================================================================
=====================================================================================================================================================================================================
ACTIVE CALC from raw log:
*************************