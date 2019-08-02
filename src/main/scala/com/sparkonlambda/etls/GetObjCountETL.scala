package com.sparkonlambda.etls


import com.sparkonlambda.etls.utils.GetSparkSession

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
