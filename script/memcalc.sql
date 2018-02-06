~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit --class vng.stats.ub.tezt.SparkJoin --master yarn --deploy-mode cluster --queue production --driver-memory 1024m --conf spark.yarn.driver.memoryOverhead=384 --executor-memory 1024m --executor-cores 1 --num-executors 1 --conf spark.yarn.executor.memoryOverhead=384 --files hdfs:///user/spark/share/lib/lib_20161017014545/hive-site.xml /home/fairy/vinhdp/stats-spark.jar FAIR 2016-11-01 3000

container:	2
core:		2
ram:		3G

Will allocate AM container, with 1408 MB memory including 384 MB overhead
Registering block manager 10.60.43.16:41539 with 457.9 MB RAM, BlockManagerId(driver, 10.60.43.16, 41539)
Will request 1 executor containers, each with 1 cores and 1408 MB memory including 384 MB overhead
Registering block manager c404.hadoop.gda.lo:20641 with 511.1 MB RAM, BlockManagerId(1, c404.hadoop.gda.lo, 20641)

driver, excutor:
	1024					=> 1024
	max(1024 * 0.07, 384)	=> 384
	-----------------------------------------
							=> 1408
							=> yarn: 1536

~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit --class vng.stats.ub.tezt.SparkJoin --master yarn --deploy-mode cluster --queue production --driver-memory 1024m --conf spark.yarn.driver.memoryOverhead=384 --executor-memory 2048m --executor-cores 1 --num-executors 1 --conf spark.yarn.executor.memoryOverhead=384 --files hdfs:///user/spark/share/lib/lib_20161017014545/hive-site.xml /home/fairy/vinhdp/stats-spark.jar FAIR 2016-11-01 3000

driver:
	1024					=> 1024
	max(1024 * 0.07, 384)	=> 384
	-----------------------------------------
							=> 1408
							=> yarn: 1536

driver:
	2048					=> 2048
	max(1024 * 0.07, 384)	=> 384
	-----------------------------------------
							=> 2432
							=> yarn: 2688

~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit --class vng.stats.ub.tezt.SparkJoin --master yarn --deploy-mode cluster --queue production --driver-memory 2048m --conf spark.yarn.driver.memoryOverhead=384 --executor-memory 2048m --executor-cores 1 --num-executors 1 --conf spark.yarn.executor.memoryOverhead=384 --files hdfs:///user/spark/share/lib/lib_20161017014545/hive-site.xml /home/fairy/vinhdp/stats-spark.jar FAIR 2016-11-01 3000

driver:
	2048					=> 2048
	max(1024 * 0.07, 384)	=> 384
	-----------------------------------------
							=> 2432
							=> yarn: 2688

driver:
	2048					=> 2048
	max(1024 * 0.07, 384)	=> 384
	-----------------------------------------
							=> 2432
							=> yarn: 2688

~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit --class vng.stats.ub.tezt.SparkJoin --master yarn --deploy-mode cluster --queue production --driver-memory 2048m --conf spark.yarn.driver.memoryOverhead=384 --executor-memory 2500m --executor-cores 1 --num-executors 1 --conf spark.yarn.executor.memoryOverhead=384 --files hdfs:///user/spark/share/lib/lib_20161017014545/hive-site.xml /home/fairy/vinhdp/stats-spark.jar FAIR 2016-11-01 3000

driver:
	2048					=> 2048
	max(1024 * 0.07, 384)	=> 384
	-----------------------------------------
							=> 2432
							=> yarn: 2688
excutor:
	2500					=> 2500
	max(1024 * 0.07, 384)	=> 384
	-----------------------------------------
							=> 2884
							=> yarn: 3072

~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit --class vng.stats.ub.tezt.SparkJoin --master yarn --deploy-mode cluster --queue production --driver-memory 1024m --conf spark.yarn.driver.memoryOverhead=1152 --executor-memory 1500m --executor-cores 1 --num-executors 1 --conf spark.yarn.executor.memoryOverhead=1536 --files hdfs:///user/spark/share/lib/lib_20161017014545/hive-site.xml /home/fairy/vinhdp/stats-spark.jar FAIR 2016-11-01 3000
driver:
	1024					=> 1024
	max(1024 * 0.07, 384)	=> 1152
	-----------------------------------------
							=> 2176
							=> yarn: 2304 = 384 x 6
excutor:
	1500					=> 1500
	max(1024 * 0.07, 384)	=> 384
	-----------------------------------------
							=> 1884
							=> yarn: 1920 = 384 x 5
final: 4G
------------------------------------------------------------------------------------------
driver:
	1024					=> 1024
	max(1024 * 0.07, 384)	=> 768
	-----------------------------------------
							=> 1792
							=> yarn: 1920 = 384 x 5
excutor:
	1500					=> 1500
	max(1024 * 0.07, 384)	=> 384
	-----------------------------------------
							=> 1884
							=> yarn: 1920 = 384 x 5
final: 3G
------------------------------------------------------------------------------------------
driver:
	1024					=> 1024
	max(1024 * 0.07, 384)	=> 768
	-----------------------------------------
							=> 1792
							=> yarn: 1920 = 384 x 5
excutor:
	1500					=> 1500
	max(1024 * 0.07, 384)	=> 1536
	-----------------------------------------
							=> 3036
							=> yarn: 3072 = 384 x 8
final: 4G
------------------------------------------------------------------------------------------
driver:
	1024					=> 1024
	max(1024 * 0.07, 384)	=> 1152
	-----------------------------------------
							=> 2176
							=> yarn: 2304 = 384 x 6
excutor:
	1500					=> 1500
	max(1024 * 0.07, 384)	=> 1536
	-----------------------------------------
							=> 3036
							=> yarn: 3072 = 384 x 8
final: 5G
------------------------------------------------------------------------------------------
~/ub/tools/spark-1.6.2-scala.2.11/bin/spark-submit --class vng.stats.ub.tezt.SparkJoin --master yarn --deploy-mode cluster --queue production --driver-memory 1024m --conf spark.yarn.driver.memoryOverhead=1152 --executor-memory 1024m --executor-cores 1 --num-executors 4 --conf spark.yarn.executor.memoryOverhead=384 --files hdfs:///user/spark/share/lib/lib_20161017014545/hive-site.xml /home/fairy/vinhdp/stats-spark.jar FAIR 2016-11-01 3000
driver:
	1024					=> 1024
	max(1024 * 0.07, 384)	=> 1152
	-----------------------------------------
							=> 1792
							=> yarn: 1920 = 384 x 5
4 excutor:
	1024					=> 1024
	max(1024 * 0.07, 384)	=> 384
	-----------------------------------------
							=> 1408
							=> yarn: 1536 = 384 x 4
8064 => 7G