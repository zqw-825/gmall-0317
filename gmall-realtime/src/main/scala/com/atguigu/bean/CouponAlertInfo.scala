package com.atguigu.bean

/**
 * @author zqw
 * @create 2020-08-19 11:58 
 */
case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)
