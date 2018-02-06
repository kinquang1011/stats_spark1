=================================================================================================================
** READ FILE NAME: http://stackoverflow.com/questions/29686573/spark-obtaining-file-name-in-rdds

import org.apache.spark.sql.functions.input_file_name

val inputPath: String = ???

sqlContext.read.text(inputPath)
  .select(input_file_name, $"value")
  .as[(String, String)] // Optionally convert to Dataset
  .rdd // or RDD

=================================================================================================================