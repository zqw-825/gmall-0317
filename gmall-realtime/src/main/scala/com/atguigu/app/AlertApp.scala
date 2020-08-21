package com.atguigu.app

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
 * @author zqw
 * @create 2020-08-19 13:49 
 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaStream =
      MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_EVENT, ssc)

    //提前定义一个时间格式
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")


    //封装成 样例类DStream     DStream[EventLog]
    val eventLogDstream = kafkaStream.map(record => {
      //对数据流进行处理：解析JSON为样例类
      val eventLog = JSON.parseObject(record.value(), classOf[EventLog])
      //b.处理日志日期和小时
      val str = sdf.format(new Date(eventLog.ts))
      val date = str.split(" ")


      eventLog.logDate = date(0)
      eventLog.logHour = date(1)

      eventLog
    })
//    eventLogDstream.cache()
//    eventLogDstream.print()

    //4.开5min窗
    val eventLogDStream = eventLogDstream.window(Seconds(10))

    //5.按照mid做分组处理
    val eds = eventLogDStream.map(e => (e.mid, e)).groupByKey()

    //6.对单条数据做处理:
    //6.1 三次及以上用不同账号(登录并领取优惠劵)：对uid去重
    //6.2 没有浏览商品：反面考虑,如果有浏览商品,当前mid不产生预警日志
    val couponAlertInfoDStream = eds.map { case (mid, iter) => {
      //创建Set用于存放领券的UID
      val uids = new java.util.HashSet[String]()
      //创建Set用于存放领券涉及的商品ID
      val itemIds = new java.util.HashSet[String]()
      //创建List用于存放用户行为
      val events = new java.util.ArrayList[String]()
      //定义一个 标志
      var noClick = true

      breakable {
        iter.foreach(eventLog => {
          //提取  事件evid
          val evid = eventLog.evid
          //加入evidList
          events.add(evid)
          //如果领券加入uidset
          if ("coupon".equals(evid)) {
            uids.add(eventLog.uid)
            itemIds.add(eventLog.itemid)
          } else if ("clickItem".equals(evid)) {
            noClick = false
            //优化
            break()
          }
        })
      }

      //按条件筛选写出到 样例类CouponAlertInfo
      if (uids.size() >= 3 && noClick) {
        CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis())
      } else {
        null
      }
    }
    }
//    couponAlertInfoDStream.cache()
//    couponAlertInfoDStream.print()
    //过滤null值
    val filterCouponAlertInfoDStream = couponAlertInfoDStream.filter(e => e != null)

    filterCouponAlertInfoDStream.cache()
    filterCouponAlertInfoDStream.print()




    //将数据写入ES  foreachRDD
    filterCouponAlertInfoDStream.foreachRDD(rdd => {
      //foreachPartition 减少与ES的连接数
      rdd.foreachPartition(iter => {

        //准环结构
        val docIdToDate = iter.map(alertInfo => {
          val minutes = alertInfo.ts / 1000 / 60
          (s"${alertInfo.mid}-$minutes", alertInfo)
        })
        //获取当前时间
        val date = LocalDate.now()
        MyEsUtil.insertByBulk(GmallConstants.GMALL_COUPON_ALERT_PRE + "_" + date,
          "_doc",
          docIdToDate.toList)
      })
    })

    //启动任务
    ssc.start()
    ssc.awaitTermination()


  }

}
