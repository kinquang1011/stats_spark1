package vng.stats.ub.common

import vng.stats.ub.utils.Constants

object DbFieldName {
  
  val KpiIds = Map(
      
      /** USER KPI ID **/
      Constants.Kpi.ACTIVE                     -> Array(10000,50000),
      Constants.Kpi.NEW_PLAYING                -> Array(-1000,51000),
      Constants.Kpi.NEW_ACCOUNT_PLAYING        -> Array(11000,-1000),
      Constants.Kpi.NEW_ROLE_PLAYING           -> Array(12000,-1000),
      Constants.Kpi.RETENTION_PLAYING          -> Array(13000,52000),
      Constants.Kpi.CHURN_PLAYING              -> Array(14000,53000),

      Constants.Kpi.PAYING_USER                -> Array(15000,54000),
      Constants.Kpi.NET_REVENUE                -> Array(16000,55000),
      Constants.Kpi.RETENTION_PAYING           -> Array(17000,56000),
      Constants.Kpi.CHURN_PAYING               -> Array(18000,57000),
      Constants.Kpi.NEW_PAYING                 -> Array(19000,58000),
      Constants.Kpi.NEW_PAYING_NET_REVENUE     -> Array(20000,59000),
      
      Constants.Kpi.NRU                        -> Array(21000,60000),
      Constants.Kpi.NGR                        -> Array(22000,61000),
      Constants.Kpi.RR                         -> Array(23000,62000),
      Constants.Kpi.CR                         -> Array(24000,63000),
      
      Constants.Kpi.NEW_USER_PAYING            -> Array(25000,64000),
      Constants.Kpi.NEW_USER_PAYING_NET_REVENUE-> Array(26000,65000),
      
      Constants.Kpi.NEW_USER_RETENTION         -> Array(27000,66000),
      Constants.Kpi.NEW_USER_RETENTION_RATE    -> Array(28000,67000),
      Constants.Kpi.RETENTION_PLAYING_RATE     -> Array(29000,68000),
      
      Constants.Kpi.ACU                        -> Array(30000,69000),
      Constants.Kpi.PCU                        -> Array(31000,70000),
      
      Constants.Kpi.RETENTION_PAYING_RATE      -> Array(32000,71000),
      Constants.Kpi.SERVER_NEW_ACCOUNT_PLAYING -> Array(33000,72000),
      
      Constants.Kpi.PLAYING_TIME               -> Array(34000,73000),
      Constants.Kpi.USER_RETENTION             -> Array(35000,74000),
      Constants.Kpi.AVG_PLAYING_TIME           -> Array(36000,75000),
      Constants.Kpi.CONVERSION_RATE            -> Array(37000,75000),
      Constants.Kpi.ARRPU                      -> Array(38000,75000),
      Constants.Kpi.ARRPPU                     -> Array(39000,75000),
      
      // other from 52
      Constants.Kpi.GROSS_REVENUE              -> Array(52000,75000),
      Constants.Kpi.NEW_PAYING_GROSS_REVENUE   -> Array(53000,75000),
      Constants.Kpi.NEW_USER_PAYING_GROSS_REVENUE-> Array(54000,65000)
  )
  
  val Timings = Map(
      Constants.Timing.A1     -> 1,
      Constants.Timing.A7     -> 7,
      Constants.Timing.A30    -> 30,
      Constants.Timing.A60    -> 60,
      Constants.Timing.A90    -> 90,
      Constants.Timing.A180    -> 180,
      
      Constants.Timing.AC1   -> 11,
      Constants.Timing.AC7   -> 17,
      Constants.Timing.AC30   -> 31,
      Constants.Timing.AC60   -> 61,
      
      Constants.Timing.A2     -> 2,
      Constants.Timing.A3     -> 3,
      Constants.Timing.A14    -> 14
  )
}