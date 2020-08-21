package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zqw
 * @create 2020-08-16 20:35 
 */
object DauApp {
  def main(args: Array[String]): Unit = {
    //创建SparkConf
    val conf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    //创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //读取kafka START主题数据  创建流
    val kafkaDStream =
      MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_START, ssc)
    //读取的数据为json格式
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    //转换为样例类对象
    val startLogStream = kafkaDStream.map(record => {
      //取出value
      val value = record.value()
      //转换为样例类对象
      val startUpLog = JSON.parseObject(value, classOf[StartUpLog])
      //取出时间戳字段解析给为logDate和logHour赋值
      val ts = startUpLog.ts
      val dateHour = sdf.format(new Date(ts))
      val dateHourArr = dateHour.split(" ")
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)
      startUpLog
    })

    startLogStream.cache()
    startLogStream.count().print()

    //跨批次去重
    val filterByRedisDSteam = DauHandler.filterByRedis(startLogStream,ssc.sparkContext)

    filterByRedisDSteam.cache()
    filterByRedisDSteam.count().print()

    //同批次去重
    val filterByGroupDStream = DauHandler.filterByGroup(filterByRedisDSteam)


    filterByGroupDStream.cache()
    filterByGroupDStream.count().print()

    //写入redis
    DauHandler.saveMidToRedis(filterByGroupDStream)


    //写入Hbase
    filterByGroupDStream.foreachRDD(
      rdd=>{
        rdd.saveToPhoenix(
          "GMALL200317_DAU",
          classOf[StartUpLog].getDeclaredFields.map(_.getName.toUpperCase()),
          HBaseConfiguration.create(),
          Some("hadoop102,hadoop103,hadoop104:2181")
        )
      }
    )

    //启动任务

    ssc.start()
    ssc.awaitTermination()


  }

}
