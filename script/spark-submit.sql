/usr/bin/spark-submit --class vng.stats.ub.normalizer.v2.G10SeaFormatter --master yarn --deploy-mode cluster --queue production --driver-memory 1g --executor-memory 1g --executor-cores 4 --num-executors 1 stats-spark.jar log_date=2016-09-06 extraTime=25200000

/usr/bin/spark-submit --class vng.stats.ub.normalizer.v2.stctFormatter --master yarn --deploy-mode cluster --queue production --driver-memory 2g --executor-memory 3g --executor-cores 2 --num-executors 1 --conf spark.yarn.executor.memoryOverhead=512 /tmp/vinhdp/stats-spark.jar logDate=2016-09-01

/usr/bin/spark-submit --class vng.stats.ub.normalizer.v2._3qmFormatter --master yarn --deploy-mode cluster --queue production --driver-memory 3g --executor-memory 6g --executor-cores 2 --num-executors 2 --conf spark.yarn.executor.memoryOverhead=512 --conf spark.shuffle.memoryFraction=0 /tmp/vinhdp/stats-spark.jar logDate=2016-09-01

/usr/bin/spark-submit --class vng.stats.ub.normalizer.v2.SkyGardenGlobalFormatter --master yarn --deploy-mode cluster --queue production --driver-memory 3g --executor-memory 6g --executor-cores 2 --num-executors 2 /tmp/vinhdp/stats-spark.jar logDate=2016-12-01

/usr/bin/spark-submit --class vng.stats.ub.normalizer.v2.StonyVnFormatter --master yarn --deploy-mode cluster --queue production --driver-memory 3g --executor-memory 6g --executor-cores 2 --num-executors 2 /tmp/vinhdp/stats-spark.jar logDate=2016-12-01

sh run.sh > log.txt 2>&1 &

ps faxu | grep run.sh

/usr/bin/spark-submit --class vng.stats.ub.report3.jobsubmit.Rerun --master yarn --deploy-mode cluster --queue production --driver-memory 3g --executor-memory 6g --executor-cores 2 --num-executors 2 /tmp/vinhdp/stats-spark.jar run_type=run from_date=2016-01-02 to_date=2016-01-01 game_code=stct log_date=2016-12-01 calc_id=id source=ingame group_id=sid ccu_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/stct/ub/sdk_data/ccu_2 activity_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/stct/ub/sdk_data/activity_2 acc_reg_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/stct/ub/sdk_data/accregister_2 payment_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/stct/ub/sdk_data/payment_2 firstcharge_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/stct/ub/sdk_data/first_charge_2 job_name=spark-stct-game-all-kpi-2016-12-01 report_number=7 run_timing=a1

org.apache.spark.sql.hive.HiveContext

val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
val df = sqlContext.sql("FROM kv.loginlogout SELECT account_id, account_name, role_id, role_name WHERE ds='2016-09-30'")
df.distinct().coalesce(1).registerTempTable("loginlogout_t")
sqlContext.sql("INSERT OVERWRITE TABLE kv_out.loginlogout PARTITION (dt='2016-09-30') SELECT * FROM loginlogout_t")

Config History Server

~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit --class vng.stats.ub.normalizer.v2.SupperFarmFormatter --master yarn --deploy-mode cluster --queue default --driver-memory 1000m --executor-memory 15000m --executor-cores 1 --num-executors 1 --files hdfs:///user/spark/share/lib/lib_20161017014545/hive-site.xml,hdfs:///user/spark/share/lib/lib_20161017014545/spark-defaults.conf  --jars /tmp/jar/lib/sdk_datanucleus-core-3.2.10.jar,/tmp/jar/lib/sdk_datanucleus-api-jdo-3.2.6.jar,/tmp/jar/lib/sdk_datanucleus-rdbms-3.2.9.jar /tmp/jar/s10/stats-spark.jar gameCode=sfmgsn logDate=2016-10-18


~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit --class vng.stats.ub.tezt.Spark --master yarn --deploy-mode cluster --queue production --driver-memory 1g --executor-memory 2g --executor-cores 2 --num-executors 2 --files hdfs:///user/spark/share/lib/lib_20161017014545/hive-site.xml /home/fairy/vinhdp/stats-spark.jar


~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit --class vng.stats.ub.report3.jobsubmit.Rerun --master yarn --deploy-mode cluster --queue production --driver-memory 1g --executor-memory 1g --executor-cores 1 --num-executors 2 /home/fairy/vinhdp/stats-spark.jar run_type=run from_date=2016-01-02 to_date=2016-01-01 game_code=stct log_date=2016-09-01 calc_id=id source=ingame group_id=os ccu_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/stct/ub/sdk_data/ccu_2 activity_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/stct/ub/sdk_data/activity_2 acc_reg_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/stct/ub/sdk_data/accregister_2 payment_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/stct/ub/sdk_data/payment_2 firstcharge_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/stct/ub/sdk_data/first_charge_2 job_name=spark-stct-game-all-kpi-2016-12-01 report_number=3 run_timing=a1


DEVICE
~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit --class vng.stats.ub.report2.jobsubmit.Rerun --master yarn --deploy-mode cluster --queue production --driver-memory 1g --executor-memory 1g --executor-cores 1 --num-executors 2 /home/fairy/vinhdp/stats-spark.jar run_type=run from_date=2016-01-02 to_date=2016-01-01 game_code=stct log_date=2016-12-01 calc_id=did source=ingame ccu_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/stct/ub/sdk_data/ccu_2 activity_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/stct/ub/sdk_data/activity_2 acc_reg_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/stct/ub/sdk_data/device_register_2 payment_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/stct/ub/sdk_data/payment_2 firstcharge_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/stct/ub/sdk_data/device_first_charge_2 job_name=spark-stct-game-all-kpi-2016-12-01 report_number=2 run_timing=a1


SDK ETL:
~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit --class vng.stats.ub.normalizer.v2.SdkFormatter --master yarn --deploy-mode cluster --queue production --driver-memory 3000m --executor-memory 12000m --executor-cores 2 --num-executors 2 --files hdfs:///user/spark/share/lib/lib_20161017014545/hive-site.xml,hdfs:///user/spark/share/lib/lib_20161017014545/spark-defaults.conf  --jars /tmp/jar/lib/datanucleus-core-3.2.10.jar,/tmp/jar/lib/datanucleus-api-jdo-3.2.6.jar,/tmp/jar/lib/datanucleus-rdbms-3.2.9.jar /home/fairy/vinhdp/stats-spark.jar logDate=2016-12-14 gameCode=ddd2mp2  outputFolder=sdk_data inputPath=/ge/gamelogs/sdk extraTime=0

HCATALOG ETL:
~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit --class vng.stats.ub.normalizer.hcatalog.HptFormatter --master yarn --deploy-mode cluster --queue production --driver-memory 1g --executor-memory 1g --executor-cores 2 --num-executors 3 --files hdfs:///user/spark/share/lib/lib_20161017014545/hive-site.xml,hdfs:///user/spark/share/lib/lib_20161017014545/spark-defaults.conf  --jars /tmp/jar/lib/datanucleus-core-3.2.10.jar,/tmp/jar/lib/datanucleus-api-jdo-3.2.6.jar,/tmp/jar/lib/datanucleus-rdbms-3.2.9.jar /home/fairy/vinhdp/stats-spark.jar logDate=2016-12-14

SDK GAME REPORT:
~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit --class vng.stats.ub.report2.jobsubmit.Rerun --master yarn --deploy-mode cluster --queue production --driver-memory 1g --executor-memory 1g --executor-cores 1 --num-executors 4 /home/fairy/vinhdp/stats-spark.jar run_type=run from_date=2016-01-02 to_date=2016-01-01 game_code=ddd2mp2 log_date=2016-12-14 calc_id=id source=sdk ccu_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/ddd2mp2/ub/sdk_data/ccu_2 activity_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/ddd2mp2/ub/sdk_data/activity_2 acc_reg_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/ddd2mp2/ub/sdk_data/accregister_2 payment_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/ddd2mp2/ub/sdk_data/payment_2 firstcharge_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/ddd2mp2/ub/sdk_data/first_charge_2 job_name=spark-ddd2mp2-game-all-kpi-2016-12-14 report_number=2-3-4-5-6-7-8-9 run_timing=a1,a3,a7,a14,a30,ac7,ac30

GAME REPORT:
~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit --class vng.stats.ub.report2.jobsubmit.Rerun --master yarn --deploy-mode cluster --queue production --driver-memory 2g --executor-memory 1g --executor-cores 2 --num-executors 3 /home/fairy/vinhdp/stats-spark.jar run_type=run from_date=2016-01-02 to_date=2016-01-01 game_code=hpt log_date=2016-12-14 calc_id=id source=ingame ccu_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/hpt/ub/data/ccu_2 activity_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/hpt/ub/data/activity_2 acc_reg_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/hpt/ub/data/accregister_2 payment_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/hpt/ub/data/payment_2 firstcharge_path=hdfs://c408.hadoop.gda.lo:8020/ge/warehouse/hpt/ub/data/first_charge_2 job_name=spark-hpt-game-all-kpi-2016-12-14 report_number=2-3-4-5-6-7-8-9 run_timing=a1,a3,a7,a14,a30,ac7,ac30