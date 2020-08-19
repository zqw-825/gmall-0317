package com.atguigu.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * @author zqw
 * @create 2020-08-18 19:42 
 */
object GmvApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("GmvApp")
    val scc = new StreamingContext(conf, Seconds(5))
    val kafkaStream = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO, scc)
    val orderInfoStream = kafkaStream.map(rdd => {
      val info = JSON.parseObject(rdd.value(), classOf[OrderInfo])
      val create_time = info.create_time

      val strings = create_time.split(" ")
      info.create_date = strings(0)
      info.create_hour = strings(1).split(":")(0)

      val tuple = info.consignee_tel.splitAt(4)

      info.consignee_tel = tuple._1 + "*******"

      info

    })

    orderInfoStream.cache()
    orderInfoStream.print()

    //写入phinix
    orderInfoStream.foreachRDD(rdd =>{
      rdd.saveToPhoenix("GMALL2020_ORDER_INFO",
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    scc.start()
    scc.awaitTermination()

  }

}
