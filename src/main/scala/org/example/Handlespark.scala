package org.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Handlespark extends App {
  println("Hello, handle-spark")

  val logFile = "./test.txt"
  val conf = new SparkConf().setAppName("Simple Application")
  .setMaster("local")
  val sc = new SparkContext(conf)

  val logData = sc.textFile(logFile)      // sparkからログの取得
  val counts = logData.filter(_.nonEmpty) // 空の行は集計しない
      .map(line => line .split("\t")(1))    // タブ区切りの1つ目をkeyにする(user)
      .map(word => (word, 1))               // keyに対してvalueを1にする
      .reduceByKey(_ + _)                   // valueを足す集計処理

  // valueの値が高い順にソート
  val sortCount = counts.collect.toSeq.sortBy(_._2).toSeq.reverse

  // 出力
  for (i <- sortCount.take(30))
    println(i)

  // 集計した値をさらにsumというkeyで集計（ユニーク数となる）
  val countSum = counts.map(word => ("sum", 1))
    .reduceByKey(_ + _)

  for (i <- countSum.take(10))
    println("unique_sum: " + i)
}
