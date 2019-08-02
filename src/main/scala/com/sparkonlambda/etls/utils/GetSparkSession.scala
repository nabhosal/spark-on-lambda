package com.sparkonlambda.etls.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * GetSparkSession
  */

object GetSparkSession {

  def get: SparkSession = {
    val conf = new SparkConf().setAppName("Spark-on-lambda")
    conf.setMaster("local[*]")
    // conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.set("spark.hadoop.fs.s3a.access.key", System.getenv("S3N_AWS_ACCESSKEY"))
    conf.set("spark.hadoop.fs.s3a.secret.key", System.getenv("S3N_AWS_SECRETKEY"))
    conf.set("fs.s3a.endpoint", "s3-ap-south-1.amazonaws.com")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("com.amazonaws.services.s3a.enableV4", "true")
    conf.set("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
    conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
    // conf.set("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider, com.amazonaws.auth.EnvironmentVariableCredentialsProvider, org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider");
    conf.set("spark.sql.parquet.filterPushdown", "true")
    conf.set("spark.sql.parquet.mergeSchema", "false")
    // conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2");
    conf.set("spark.speculation", "false")
    conf.set("spark.debug.maxToStringFields", "100")

    val sparkSession = SparkSession.builder.config(conf).getOrCreate
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", System.getenv("S3N_AWS_ACCESSKEY"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", System.getenv("S3N_AWS_SECRETKEY"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3-ap-south-1.amazonaws.com")
    sparkSession.sparkContext.hadoopConfiguration.set("com.amazonaws.services.s3a.enableV4", "true")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.multiobjectdelete.enable", "false")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.buffer.dir", "/tmp")

    sparkSession
  }
}
