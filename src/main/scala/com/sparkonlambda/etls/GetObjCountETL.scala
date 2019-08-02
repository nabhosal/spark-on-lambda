package com.sparkonlambda.etls

import com.sparkonlambda.etls.utils.GetSparkSession

/**
  * GetObjCountETL is demo ETL job for illustrating basic etl
  * Input is map
  * {
  *   "type": "GetObjCountETL",
  *   "input_path": "s3a://bucket_name/dumps/144-26Jul2019-100424/",
  *   "output_dir": "s3a://bucket_name/spark-on-lambda/write/smallfiles/"
  *   "source_ds_type": "json"
  * }
  * it return total row in data-frame
  */
class GetObjCountETL extends Serializable {

  def func(map: java.util.Map[String, Object]): java.util.Map[String, Object] = {

    val sparkSession = GetSparkSession.get

    val ds_type = map.get("source_ds_type")

    val df1 = ds_type match {
      case "csv"   => sparkSession.read.csv(String.valueOf(map.get("input_path")))
      case "json"   => sparkSession.read.json(String.valueOf(map.get("input_path")))
    }

    val tot = df1.count();
    println(tot)
    map.put("total_rows", tot.toString)
    map
  }

}
