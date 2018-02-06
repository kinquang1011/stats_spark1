package vng.stats.ub.report

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.control.Exception.allCatch
import scala.util.control.Breaks._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import vng.stats.ub.utils.DataUtils
import java.util.Calendar
import java.util.Locale
import java.text.SimpleDateFormat
import vng.stats.ub.utils.DateTimeUtils
case class PromotionStats(
    id:String
    , game:String
    , start:String
    , end:String
    , phase:Int
    , revenue: Long
    , pu:Long
    , usersPromotion:Long
    , usersLoginPromotion:Long
    , usersLogin:Long
    , hdfsLoginFileCount:Int
    , hdfsPayFileCount:Int
    )
case class A(id:String)
object Promotion {
  //spark-shell --jars /home/zdeploy/ub/spark/core2/lib/spark-csv_2.11-1.3.0.jar,/home/zdeploy/ub/spark/core2/lib/commons-csv-1.1.jar,/home/zdeploy/ub/stats-spark.jar
  def report(sqlContext:SQLContext,sc:SparkContext,logDate:String, inputPromotionPath:String, inputPaymentPath:String,inputLoginPath:String, outPath:String ){
    
    /*
     val logDate ="2016-05-17"
    val inputPromotionPath ="/tmp/promotion"
    val inputPaymentPath ="/ge/warehouse/global/ub/data/payment"
    val inputLoginPath ="/ge/warehouse"
    val outPath ="/tmp/preport"
    */
    val progam ="/program"
    val account ="/account"
    
    val inFilePCalc = inputPromotionPath + progam + "/active/" + logDate
    val pDF = sqlContext.read.parquet(inFilePCalc)
    
    val calendar = Calendar.getInstance(Locale.UK)
    val format = new SimpleDateFormat("yyyy-MM-dd")
    calendar.add(Calendar.DATE, -1)
    val dCT = calendar.getTime()
    
    
  def remNotExists(files:String): String = {
    val sb = new StringBuilder
    val lstFiles = files.split(",")
    val fs = FileSystem.get(new Configuration(true))
    var rs=true
    for (i <- 0 to lstFiles.size - 1) {
      val path = new Path(lstFiles(i)+"/_SUCCESS")
      if (fs.exists(path)) {
        if(rs){
          sb.append(lstFiles(i))
          rs = false
        }else{
          sb.append(",")
          sb.append(lstFiles(i))
        }
      }
    }
    sb.toString()
  }
  
    def getLoginDataSet(files:String):org.apache.spark.sql.DataFrame ={
      if(files.length()>5){
        val s = sc.textFile(files)
        val df = sqlContext.createDataFrame(s.map { x => A(x) }).repartition(3)
        return df
      }
      return null
    }
    def getGameCode(promotionGameCode:String):String ={
      val gc = promotionGameCode match {
        case "boom" => "bnb"
        case "zg"=> "gn"
        case "kt2" =>"wjx2"
        case "9t" =>"ct"
        case "tdtk" =>"td"
        case "ks3d" =>"ks"
        case "bc_2" =>"bc2"
        case _ => promotionGameCode
      }
      gc
    }
    
    val arrs = scala.collection.mutable.ArrayBuffer.empty[PromotionStats]
    def process2(r:org.apache.spark.sql.Row, sc:SparkContext)={
      println(r)
    }
    def process(r:org.apache.spark.sql.Row, sc:SparkContext)={

      val id = r.getString(0)
      val promotionGC = r.getString(1)
      val startDate = r.getString(2)
      val endDate = r.getString(3)
      val duration = r.getInt(4)
      val phase = r.getInt(5)
      
      val outputFileA = inputPromotionPath + account + "/total/"+ logDate + "/promotion_id="+id
      //val game ="BC3"
      val promotions = sqlContext.read.parquet(outputFileA).filter("game_code='" + promotionGC +"'")
      if(!"".equals(promotionGC)){
        val gameCode = getGameCode(promotionGC)
        val lsPayFiles = phase match {
          case 0 => {
            val dST = format.parse(startDate)
            val d = ((dCT.getTime - dST.getTime)/(1000*60*60*24)).toInt
            val dCStr = format.format(dCT)
            
            DataUtils.getMultiFiles(inputPaymentPath, dCStr, 50)
          }
          case 1 =>{
            DataUtils.getMultiFiles(inputPaymentPath, startDate, duration-1)
          }
          case 2 =>{
            val dET = format.parse(endDate)
            val d = ((dCT.getTime - dET.getTime)/(1000*60*60*24)).toInt
            val dCStr = format.format(dCT)
            DataUtils.getMultiFiles(inputPaymentPath, dCStr, d-1)
          }
          case _ => ""
        }
        val loginPath = inputLoginPath +"/" + gameCode + "/a1" 
        val lsLoginFiles = phase match {
          case 0 => {
            val dST = format.parse(startDate)
            val d = ((dCT.getTime - dST.getTime)/(1000*60*60*24)).toInt
            
            val dCStr = format.format(dCT)
            DataUtils.getMultiFiles(loginPath, dCStr, d-1)
          }
          case 1 =>{
            DataUtils.getMultiFiles(loginPath, startDate, duration-1)
          }
          case 2 =>{
            val dET = format.parse(endDate)
            val d = ((dCT.getTime - dET.getTime)/(1000*60*60*24)).toInt
            val dCStr = format.format(dCT)
            DataUtils.getMultiFiles(loginPath, dCStr, d-1)
          }
          case _ => ""
        }
        
        //use lsFiles instead
        val ePayFiles = remNotExists(lsPayFiles)
        val payFiles = ePayFiles.split(",")
        val payFileCount = payFiles.length
        def getPayDataSet():org.apache.spark.sql.DataFrame ={
          if(ePayFiles.length()>5){
            val ds = sqlContext.read.parquet(payFiles:_*).filter("game_code='" + gameCode +"'").repartition(3)
            return ds
          }
          return null
        }
        val pays = getPayDataSet()
        
        val eLoginFiles = remNotExists(lsLoginFiles)
        //import org.apache.spark.sql._
        val loginFileCount = eLoginFiles.split(",").length
        val logins = getLoginDataSet(eLoginFiles)
        var pu =0L
        if(pays!=null){
          pu = pays.select("id").distinct().count()  
        }
        
        val usersPromotion = promotions.select("id").distinct().count()
        var usersLoginPromotion =0L
        var revenue=0L
        if(pu>0 && usersPromotion >0){
          val jDF = pays.as('p).join(promotions.as('pr), pays("id")===promotions("id")).select("p.id","p.game_code","p.gross_amt")
          pu = jDF.count
          if(pu>0){
            revenue = jDF.groupBy("game_code").agg(sum("gross_amt")).first().getLong(1)
          }
        }
        var userLogins = 0L
        if(usersPromotion >0 && logins!=null){
          userLogins = logins.distinct().count()
          val jLogins = promotions.as('pr).join(logins.as('l),promotions("id") === logins("id")).select("pr.id")
          usersLoginPromotion = jLogins.distinct().count()
        }
        arrs += new PromotionStats(id, gameCode, startDate, endDate, phase,revenue, pu, usersPromotion,usersLoginPromotion, userLogins, loginFileCount, payFileCount)
      }
    }
    
    pDF.collect().foreach{p => process(p,sc)}
    //arrs.foreach { x => println(x) }
    if(arrs.length>0){
      val outputFile =outPath +"/" + logDate
      val storeValues = sc.parallelize(arrs)
      val df = sqlContext.createDataFrame(storeValues)
      df.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", "\t").save(outputFile)
      arrs.clear();
    }
  }
  
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PromotionReport")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    val logDate =args(0)
    val inputPromotionPath = args(1) //"/tmp/promotion"
    val inputPaymentPath =args(2) //"/ge/warehouse/global/ub/data/payment"
    val inputLoginPath =args(3) //"/ge/warehouse"
    val outPath =args(4)//"/tmp/preport"
    report(sqlContext,sc, logDate, inputPromotionPath, inputPaymentPath,inputLoginPath,outPath )
    sc.stop()
    /*
    val payFile = "/ge/warehouse/global/ub/data/payment/2016-05-19"
    val ps = sqlContext.read.parquet(payFile)
    val allPromotionDataSet =sqlContext.read.parquet("/tmp/promotion/program/2016-05-17")
    val currentPromotionDataSet =sqlContext.read.parquet("/tmp/promotion/program/calc-2016-05-17")
    val accountPromotionDataSet =sqlContext.read.parquet("/ge/warehouse/3qmobile/ub/sdk_data/loginlogout/2016-05-24") 
     
    sqlContext.read.parquet("/ge/warehouse/promotion/program/total/2015-02-05").show
    val lsFiles = DataUtils.getMultiFiles("/ge/warehouse/global/ub/data/payment", "2016-05-20", 1)
    val ds = sqlContext.read.parquet(lsFiles)
    val payFile2 = "/ge/warehouse/global/ub/data/payment/2016-05-20"
    
    val loginFile = "/ge/warehouse/mu/a1/2016-05-20"
    val logins = sqlContext.read.format("com.databricks.spark.csv").load("har:/ge/gamelogs/promotion/2015-10-03.har/account_BC_2_choi-mu-nhan-ngay-qua-hot-lan-2.csv")
    
    
    
    val f="/ge/warehouse/jx1ctc/a1/2016-05-20,/ge/warehouse/jx1ctc/a1/a"
    val ds2 = sc.textFile(f)
    val df = sqlContext.createDataFrame(ds2.map { x => A(x) })
    
    */
    //ds2.to
    //val ds = sqlContext.read.format("com.databricks.spark.csv").load(f)

  }
  
}