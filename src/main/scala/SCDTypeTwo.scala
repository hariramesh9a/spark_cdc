/**
  * Created by arumuhx on 5/3/2018.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.{col, concat}
import org.apache.spark.sql.functions.udf


object SCDTypeTwo {

  def main(args: Array[String]): Unit = {

    if (args.length != 5) {
      println("Parameters needed: <Table A> <Table B> <id Columns> <cdc Columns> <output path to be saved>")
      sys.exit(1)
    }


    val conf = new SparkConf().setAppName("SCDType2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    //set sys.argv failures here
    val table_a = args(0)
    val yesterdayDF = sqlContext.sql(s"select * from $table_a")
    val table_b = args(1)
    val todayDF = sqlContext.sql(s"select * from $table_b")
    val idCols = args(2).toString.split(",").toSeq
    val cdcCols = args(3).toString.split(",").toSeq
    val output_path = args(4)

    // eof args parsing


    def hash(input_string: String) = {
      java.util.Base64.getEncoder.encodeToString(input_string.getBytes())
    }

    val hashId = udf((a: String) =>
      hash(a)
    )

    def isNull(n: String): String = {
      if (n == null) {
        "_x_"
      } else {
        n
      }
    }

    val defaultNull = udf((a: String) =>
      isNull(a)
    )

    /* Get all columns needed for determining CDC - IDs for determine whats new and old. and Column list for comparing in CDC*/


    val hashYesterdayDF = yesterdayDF.withColumn("yesterdayHashId", hashId(concat(idCols.map(c => defaultNull(col(c))): _*))).withColumn("yesterdayCdcId", hashId(concat(cdcCols.map(c => defaultNull(col(c))): _*)))
    val hashTodayDF = todayDF.withColumn("todayHashId", hashId(concat(idCols.map(c => defaultNull(col(c))): _*))).withColumn("todayCdcId", hashId(concat(cdcCols.map(c => defaultNull(col(c))): _*)))

    var colvals = hashTodayDF.schema.fieldNames
    val lookup = colvals.map(c => c + "_1").toSeq
    val hashTodayRenamedDf = hashTodayDF.toDF(lookup: _*)

    val partitionedYesterdayDF = hashYesterdayDF.repartition($"yesterdayHashId")
    val partitionedTodayDF = hashTodayRenamedDf.repartition($"todayHashId_1")


    val joinedDF = partitionedYesterdayDF.join(partitionedTodayDF, partitionedYesterdayDF.col("yesterdayHashId") === partitionedTodayDF.col("todayHashId_1"), "full_outer").withColumn("yesterdaysHashID", partitionedYesterdayDF.col("yesterdayHashId"))
      .withColumn("todaysHashID", partitionedTodayDF.col("todayHashId_1"))
      .withColumn("yesterdaysCDCID", partitionedYesterdayDF.col("yesterdayCdcId"))
      .withColumn("todaysCDCID", partitionedTodayDF.col("todayCdcId_1"))


    def isEmpty(x: String) = x == null || x.isEmpty

    val changeDetectionStatus = udf((a: String, b: String) =>
      (a, b) match {
        case (a, b) if !isEmpty(a) && isEmpty(b) => "I"
        case (a, b) if isEmpty(a) && !isEmpty(b) => "D"
        case _ => "N"
      }
    )

    val changeDataCaptureStatus = udf((a: String, b: String) =>
      (a, b) match {
        case (a, b) if a == b => "N"
        case _ => "U"
      }
    )

    val cdcStatus = udf((a: String, b: String) =>
      (a, b) match {
        case (a, b) if a == "I" || a == "D" => a
        case (a, b) if a == "N" || b == "U" => b
        case _ => a
      }
    )

    val resultDF = joinedDF.withColumn("insert_delete_flag", changeDetectionStatus(joinedDF.col("todaysHashID"), joinedDF.col("yesterdaysHashID")))
      .withColumn("updated_flag", changeDataCaptureStatus(joinedDF.col("todaysCDCID"), joinedDF.col("yesterdaysCDCID")))
    val saveDF = resultDF.withColumn("cdc_status", cdcStatus(resultDF.col("insert_delete_flag"), resultDF.col("updated_flag"))).drop(resultDF.col("updated_flag"))
      .drop(resultDF.col("insert_delete_flag")).drop(resultDF.col("yesterdaysHashID")).drop(resultDF.col("todaysHashID")).drop(resultDF.col("yesterdaysCDCID"))
      .drop(resultDF.col("todaysCDCID"))
      .drop(resultDF.col("yesterdayHashId"))
      .drop(resultDF.col("todayHashId_1"))
      .drop(resultDF.col("todayCdcId_1"))
      .drop(resultDF.col("yesterdayCdcId"))
    saveDF.write.mode("overwrite").parquet(output_path)
    sc.stop()

  }

}
