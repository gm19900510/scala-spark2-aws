package com.gm

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @PackageName: com.gm
 * @ClassName: Spark2AWS
 * @Description: ${description}
 * @Author 高明
 * @Date 2021/8/3
 * @Version 1.0
 *
 */
object Spark2AWS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setJars(Array[String]("E:\\knowledge\\scala-spark2-aws\\target\\scala-spark2-aws-1.0-SNAPSHOT.jar"))
    conf.setAppName("Test")
    conf.setMaster("spark://192.168.3.125:7077")
    conf.set("spark.testing.memory", "2147480000")
    conf.set("spark.driver.host", "192.168.104.225")

    //s3a连接信息
    conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
    conf.set("spark.hadoop.fs.s3a.secret.key", "12345678")
    conf.set("spark.hadoop.fs.s3a.endpoint", "192.168.3.125:9010")

    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()

    //设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    val rdd = spark.sparkContext.textFile("s3a://mybucket/movies.csv")

    println(rdd.count())


    val df = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString) //也可以.option("inferSchema", true.toString) //自动推断属性列的数据类型
      .option("delimiter", ",") //分隔符，默认为 ,
      .load("s3a://mybucket/movies.csv")

    df.cache()
    df.printSchema()
    df.show()

    spark.stop()
  }
}
