package vng.stats.ub.report.jobs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import vng.stats.ub.utils.Common
import scala.util.control.Exception.allCatch
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row

object Test {

    
    def isLongNumber(s: String): Boolean = (allCatch opt s.toLong).isDefined
    
    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Test")
        conf.set("spark.hadoop.validateOutputSpecs", "false")

        var sc = new SparkContext(conf)

        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
        import sqlContext.implicits._
        
        val schemaOne = StructType(
            Array(
                    StructField("mark", IntegerType, true)
            )        
        )
        
        val schemaFive = StructType(
            Array(
                    StructField("address", StringType, true)
            )        
        )
        
        val schemaTwo = StructType(
            Array(
                    StructField("name", StringType, true), StructField("age", IntegerType, true)
            )        
        )
        
        case class SchemaOne(mark: Int)
        case class SchemaTwo(name: String, age: Int)
        
        val rddOne = sc.parallelize(0 to 5, 3).map(line => Row(line))
        val dfOne = sqlContext.createDataFrame(rddOne, schemaOne)
        dfOne.write.mode(SaveMode.Overwrite).parquet("/user/fairy/vinhdp/2016-10-01")
        
        val rddTwo = Seq(("vinhdp", 23), ("dangphucvinh", 32), ("vinh", 58), ("dang", 99))
        val dfTwo = rddTwo.toDF("name", "age")
        dfTwo.write.mode(SaveMode.Overwrite).parquet("/user/fairy/vinhdp/2016-10-02")
        
        val rddThree = Seq((1, "vinhdp", 23), (2, "dangphucvinh", 32), (3, "vinh", 58), (4, "dang", 99))
        val dfThree = rddThree.toDF("mark", "name", "age")
        dfThree.write.mode(SaveMode.Overwrite).parquet("/user/fairy/vinhdp/2016-10-03")
        
        val rddFour = Seq((1, "vinhdp"), (2, "dangphucvinh"), (3, "vinh"), (4, "dang"))
        val dfFour = rddFour.toDF("mark", "name")
        dfFour.write.mode(SaveMode.Overwrite).parquet("/user/fairy/vinhdp/2016-10-04")
        
        val rddFive = sc.parallelize(Seq(("Vinh Long"), ("HCM"), ("HN"), ("Singapore"))).map(row => Row(row))
        val dfFive = sqlContext.createDataFrame(rddFive, schemaFive)
        dfFive.write.mode(SaveMode.Overwrite).parquet("/user/fairy/vinhdp/2016-10-05")
        
        val merge = sqlContext.read.option("mergeSchema", "true").parquet("/user/fairy/vinhdp/2016-10-*")

        /*var df = sc.textFile("/user/fairy/vinhdp/tkcm.csv").map(_.split(",")).map(row => (row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString, row(5).toString)).toDF.where("_1 != 'AccountName'").selectExpr("'dptk' as game_code", "_5 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.write.mode("overwrite").format("parquet").save("/ge/warehouse/dptk/ub/data/total_login_acc_2/2016-07-18")
        
        
        df = sc.textFile("/user/fairy/vinhdp/tkcm.csv").map(_.split(",")).map(row => (row(0).toString,row(1).toString, row(2).toString, row(3).toString, row(4).toString, row(5).toString)).toDF.where("_1 != 'AccountName' and _6 != 'NULL'").selectExpr("'dptk' as game_code", "_6 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.write.mode("overwrite").format("parquet").save("/ge/warehouse/dptk/ub/data/total_paid_acc_2/2016-07-18")
        */
        
        /*var df = sqlContext.read.parquet("/ge/warehouse/dttk/ub/sdk_data/total_login_acc_2/2016-06-30")
        .write.mode("overwrite").format("parquet").save("/ge/warehouse/dttk/ub/data/total_login_acc_2/2016-06-30")
        
        df = sqlContext.read.parquet("/ge/warehouse/dttk/ub/sdk_data/total_paid_acc_2/2016-06-30")
        .write.mode("overwrite").format("parquet").save("/ge/warehouse/dttk/ub/data/total_paid_acc_2/2016-06-30")*/
        
        var df = sc.textFile("/user/fairy/vinhdp/BL_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName'").selectExpr("'pmcl' as game_code", "_4 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").write.mode("overwrite").format("parquet").save("/ge/warehouse/pmcl/ub/data/total_login_acc_2/2016-03-24")
        
        
        df = sc.textFile("/user/fairy/vinhdp/BL_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString,row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName' and _5 != 'NULL'").selectExpr("'pmcl' as game_code", "_5 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").write.mode("overwrite").format("parquet").save("/ge/warehouse/pmcl/ub/data/total_paid_acc_2/2016-03-24")
        
        
        df = sc.textFile("/user/fairy/vinhdp/DTTK_Snapshot.csv").map(_.split(",")).filter(row => isLongNumber(row(0))).map(row => (row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName'").selectExpr("'dttk' as game_code", "_4 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").write.mode("overwrite").format("parquet").save("/ge/warehouse/dttk/ub/data/total_login_acc_2/2016-03-01")
        
        
        df = sc.textFile("/user/fairy/vinhdp/DTTK_Snapshot.csv").map(_.split(",")).filter(row => isLongNumber(row(0))).map(row => (row(0).toString,row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName' and _5 != 'NULL'").selectExpr("'dttk' as game_code", "_5 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").write.mode("overwrite").format("parquet").save("/ge/warehouse/dttk/ub/data/total_paid_acc_2/2016-03-01")
        
        
        df = sc.textFile("/user/fairy/vinhdp/HTC_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName'").selectExpr("'htc' as game_code", "_4 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").write.mode("overwrite").format("parquet").save("/ge/warehouse/htc/ub/data/total_login_acc_2/2016-07-28")
        
        
        df = sc.textFile("/user/fairy/vinhdp/HTC_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString,row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName' and _5 != 'NULL'").selectExpr("'htc' as game_code", "_5 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").write.mode("overwrite").format("parquet").save("/ge/warehouse/htc/ub/data/total_paid_acc_2/2016-07-28")
        
        
        df = sc.textFile("/user/fairy/vinhdp/DMH_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName'").selectExpr("'contra' as game_code", "_4 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").write.mode("overwrite").format("parquet").save("/ge/warehouse/contra/ub/data/total_login_acc_2/2016-01-01")
        
        //df.intersect(other)
        
        
        df = sc.textFile("/user/fairy/vinhdp/DMH_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString,row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName' and _5 != 'NULL'").selectExpr("'contra' as game_code", "_5 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").write.mode("overwrite").format("parquet").save("/ge/warehouse/contra/ub/data/total_paid_acc_2/2016-01-01")
        
        
        /*df = sc.textFile("/user/fairy/vinhdp/QV_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName'").selectExpr("'wefight' as game_code", "_4 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").write.mode("overwrite").format("parquet").save("/ge/warehouse/wefight/ub/data/total_login_acc_2/2016-06-30")
        
        
        df = sc.textFile("/user/fairy/vinhdp/QV_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString,row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName' and _5 != 'NULL'").selectExpr("'wefight' as game_code", "_5 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").write.mode("overwrite").format("parquet").save("/ge/warehouse/wefight/ub/data/total_paid_acc_2/2016-06-30")
        
        
        df = sc.textFile("/user/fairy/vinhdp/TLM_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName'").selectExpr("'coccgsn' as game_code", "_4 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").write.mode("overwrite").format("parquet").save("/ge/warehouse/coccgsn/ub/data/total_login_acc_2/2016-08-01")
        
        
        df = sc.textFile("/user/fairy/vinhdp/TLM_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString,row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName' and _5 != 'NULL'").selectExpr("'coccgsn' as game_code", "_5 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").write.mode("overwrite").format("parquet").save("/ge/warehouse/coccgsn/ub/data/total_paid_acc_2/2016-08-01")
        
        df = sc.textFile("/user/fairy/vinhdp/SF_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName'").selectExpr("'sfmgsn' as game_code", "_4 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").write.mode("overwrite").format("parquet").save("/ge/warehouse/sfmgsn/ub/data/total_login_acc_2/2016-08-01")
        
        
        df = sc.textFile("/user/fairy/vinhdp/SF_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString,row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName' and _5 != 'NULL'").selectExpr("'sfmgsn' as game_code", "_5 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").write.mode("overwrite").format("parquet").save("/ge/warehouse/sfmgsn/ub/data/total_paid_acc_2/2016-08-01")
        */
        
        df = sc.textFile("/user/fairy/vinhdp/KFCV_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName'").selectExpr("'kftl' as game_code", "_4 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").where("log_date < '2016-08-11'").write.mode("overwrite").format("parquet").save("/ge/warehouse/kftl/ub/data/total_login_acc_2/2016-08-10")
        
        
        df = sc.textFile("/user/fairy/vinhdp/KFCV_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString,row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName' and _5 != 'NULL'").selectExpr("'kftl' as game_code", "_5 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").where("log_date < '2016-08-11'").write.mode("overwrite").format("parquet").save("/ge/warehouse/kftl/ub/data/total_paid_acc_2/2016-08-10")
        
        
        df = sc.textFile("/user/fairy/vinhdp/TL3D_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName'").selectExpr("'tlbbm' as game_code", "_4 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").write.mode("overwrite").format("parquet").save("/ge/warehouse/tlbbm/ub/data/total_login_acc_2/2016-01-01")

        df = sc.textFile("/user/fairy/vinhdp/TL3D_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString,row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName' and _5 != 'NULL'").selectExpr("'tlbbm' as game_code", "_5 as log_date", "_1 as id").sort("id").dropDuplicates(Seq("id"))
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").write.mode("overwrite").format("parquet").save("/ge/warehouse/tlbbm/ub/data/total_paid_acc_2/2016-01-01")
        
        //////////////////////////////////
        df = sc.textFile("/user/fairy/vinhdp/CTP_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName'").selectExpr("'ctpgsn' as game_code", "_4 as log_date", "_1 as id")
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").sort("id").dropDuplicates(Seq("id")).write.mode("overwrite").format("parquet").save("/ge/warehouse/tlbbm/ub/data/total_login_acc_2/2016-01-01")
        
        
        // TFZFBS2
        df = sc.textFile("/user/fairy/vinhdp/TFZ_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName'").selectExpr("'tfzfbs2' as game_code", "_4 as log_date", "_1 as id")
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").where("log_date < '2016-01-01'").coalesce(1).sort("id").dropDuplicates(Seq("id")).write.mode("overwrite").format("parquet").save("/ge/warehouse/tfzfbs2/ub/data/total_login_acc_2/2015-12-31")
        
        
        df = sc.textFile("/user/fairy/vinhdp/TFZ_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString,row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName' and _5 != 'NULL'").selectExpr("'tfzfbs2' as game_code", "_5 as log_date", "_1 as id")
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").where("log_date < '2016-01-01'").coalesce(1).sort("id").dropDuplicates(Seq("id")).write.mode("overwrite").format("parquet").save("/ge/warehouse/tfzfbs2/ub/data/total_paid_acc_2/2015-12-31")
    
        // Mabes
        df = sc.textFile("/user/fairy/vinhdp/Mabes_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName'").selectExpr("'10ha7ifbs1' as game_code", "_4 as log_date", "_1 as id")
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").where("log_date < '2016-04-01'").coalesce(1).sort("id").dropDuplicates(Seq("id")).write.mode("overwrite").format("parquet").save("/ge/warehouse/10ha7ifbs1/ub/data/total_login_acc_2/2016-03-31")
        
        
        df = sc.textFile("/user/fairy/vinhdp/Mabes_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString,row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName' and _5 != 'NULL'").selectExpr("'10ha7ifbs1' as game_code", "_5 as log_date", "_1 as id")
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").where("log_date < '2016-04-01'").coalesce(1).sort("id").dropDuplicates(Seq("id")).write.mode("overwrite").format("parquet").save("/ge/warehouse/10ha7ifbs1/ub/data/total_paid_acc_2/2016-03-31")
     
        // sfgsn
        df = sc.textFile("/user/fairy/vinhdp/SFWEB_Snapshot.csv").map(_.split(",")).map(row => (row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName'").selectExpr("'sfgsn' as game_code", "_4 as log_date", "_1 as id")
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").where("log_date < '2016-01-11'").coalesce(1).sort("id").dropDuplicates(Seq("id")).write.mode("overwrite").format("parquet").save("/ge/warehouse/sfgsn/ub/data/total_login_acc_2/2016-01-10")
        
        
        df = sc.textFile("/user/fairy/vinhdp/SFWEB_Snapshot.csv").map(_.split(",")).map(row => (row(2).toString,row(1).toString, row(2).toString, row(3).toString, row(4).toString)).toDF.where("_1 != 'AccountName' and _5 != 'NULL'").selectExpr("'sfgsn' as game_code", "_5 as log_date", "_1 as id")
        df.selectExpr("game_code", "substring(log_date,0,19) as log_date", "id").where("log_date < '2016-01-11'").coalesce(1).sort("id").dropDuplicates(Seq("id")).write.mode("overwrite").format("parquet").save("/ge/warehouse/sfgsn/ub/data/total_paid_acc_2/2016-01-10")
        df.write.format("com.databricks.spark.csv").save("/user/fairy/vinhdp/kv10")
    }
}