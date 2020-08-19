package com.atguigu.app

import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
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
    val scc = new StreamingContext(conf, Seconds(5))

    val kafkaStream =
      MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_EVENT, scc)

    //
    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    val eventLogDstream = kafkaStream.map(record => {
      val eventLog = JSON.parseObject(record.value(), classOf[EventLog])
      //b.处理日志日期和小时
      val date = sdf.format(eventLog.ts).split(" ")

      eventLog.logDate = date(0)
      eventLog.logHour = date(1)

      eventLog
    })

    //4.开5min窗
    val eventLogDStream = eventLogDstream.window(Minutes(5))

    //5.按照mid做分组处理
    val eds = eventLogDStream.map(e => (e.mid, e)).groupByKey()


    //6.对单条数据做处理:
    //6.1 三次及以上用不同账号(登录并领取优惠劵)：对uid去重
    //6.2 没有浏览商品：反面考虑,如果有浏览商品,当前mid不产生预警日志
    eds.map { case (mid, iter) => {

      var noClick = true
      //a.创建Set用于存放领券的UID
      val uidSet = new java.util.HashSet[String]()
      //创建Set用于存放领券涉及的商品ID
      val itemSet = new util.HashSet[String]()
      //创建List用于存放用户行为
      val evidList = new util.ArrayList[String]()

      breakable {
        iter.foreach(eventLog => {
          val evid = eventLog.evid

          evidList.add(evid)

          if ("coupon".equals(evid)) {
            uidSet.add(evid)
            itemSet.add(evid)
          } else if ("clickItem".equals(evid)) {
            noClick = false
            break()
          }
        })
      }
      if (uidSet.size() >= 3 && noClick) {
        CouponAlertInfo(mid, uidSet, itemSet, evidList, System.currentTimeMillis())
      } else {
        null
      }
    }
    }
  }

}
