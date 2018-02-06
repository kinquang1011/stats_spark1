package vng.stats.ub.common

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import vng.stats.ub.utils.Constants

object Schemas {

  val Activity = StructType(
      StructField("game_code", StringType, true) ::
      StructField("log_date", StringType, true) ::
      StructField("sid", StringType, true) ::
      StructField("id", StringType, true) ::
      StructField("rid", StringType, true) ::
      StructField("did", StringType, true) ::
      StructField("action", StringType, true) ::
      StructField("channel", StringType, true) ::
      StructField("online_time", LongType, true) ::
      StructField("level", IntegerType, true) ::
      StructField("ip", StringType, true) ::
      StructField("device", StringType, true) ::
      StructField("os", StringType, true) ::
      StructField("os_version", StringType, true) :: Nil
  )
    
  val AccountRegister = StructType(
      StructField("game_code",StringType,true) :: 
      StructField("log_date",StringType,true) ::
      StructField("package_name",StringType,true) ::
      StructField("channel",StringType,true) ::
      StructField("sid",StringType,true) ::
      StructField("id",StringType,true) ::
      StructField("ip",StringType,true) ::
      StructField("device",StringType,true) ::
      StructField("os",StringType,true) ::
      StructField("os_version",StringType,true) :: Nil
  )
  
  val ServerAccountRegister = StructType(
      StructField("game_code",StringType,true) :: 
      StructField("log_date",StringType,true) ::
      StructField("package_name",StringType,true) ::
      StructField("channel",StringType,true) ::
      StructField("sid",StringType,true) ::
      StructField("rid",StringType,true) ::
      StructField("id",StringType,true) ::
      StructField("ip",StringType,true) ::
      StructField("device",StringType,true) ::
      StructField("os",StringType,true) ::
      StructField("os_version",StringType,true) :: Nil
  )
      
  val RoleRegister = StructType(
      StructField("game_code",StringType,true) :: 
      StructField("log_date",StringType,true) ::
      StructField("id",StringType,true) ::
      StructField("sid",StringType,true) ::
      StructField("rid",StringType,true) ::
      StructField("ip",StringType,true) ::
      StructField("channel",StringType,true) ::
      StructField("device",StringType,true) ::
      StructField("os",StringType,true) ::
      StructField("os_version",StringType,true) ::
      StructField("package_name",StringType,true) :: Nil
  )
  
  val Payment = StructType(
      StructField("game_code",StringType,true) :: 
      StructField("log_date",StringType,true) ::
      StructField("package_name",StringType,true) ::
      StructField("sid",StringType,true) ::
      StructField("id",StringType,true) ::
      StructField("rid",StringType,true) ::
      StructField("level",IntegerType,true) ::
      StructField("trans_id",StringType,true) ::
      StructField("channel",StringType,true) ::
      StructField("pay_channel",StringType,true) ::
      StructField("gross_amt",DoubleType,true) ::
      StructField("net_amt",DoubleType,true) ::
      StructField("xu_instock",LongType,true) ::
      StructField("xu_spent",LongType,true) ::
      StructField("xu_topup",LongType,true) ::
      StructField("ip",StringType,true) ::
      StructField("device",StringType,true) ::
      StructField("os",StringType,true) ::
      StructField("os_version",StringType,true) :: Nil
  )
  
  val FirstCharge = StructType(
      StructField("game_code",StringType,true) :: 
      StructField("log_date",StringType,true) ::
      StructField("package_name",StringType,true) ::
      StructField("channel",StringType,true) ::
      StructField("pay_channel",StringType,true) ::
      StructField("sid",StringType,true) ::
      StructField("id",StringType,true) ::
      StructField("ip",StringType,true) ::
      StructField("device",StringType,true) ::
      StructField("os",StringType,true) ::
      StructField("os_version",StringType,true) :: Nil
  )
  
  val AccountPlayingTime = StructType(
      StructField("game_code",StringType,true) :: 
      StructField("id",StringType,true) ::
      StructField("timing",StringType,true) ::
      StructField("time",DoubleType,true) ::
      StructField("rank",StringType,true) ::
      StructField("prev_rank",StringType,true) ::
      StructField("trend",StringType,true) :: Nil
  ) 
}