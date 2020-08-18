package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.*;

/**
 * @author zqw
 * @create 2020-08-17 15:43
 */

//对输入的地址进行计算 返回正确格式的结果
@RestController
public class PublisherController {

    @Autowired
    public PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date){
        //查询日活总数
        Integer dauTotal = publisherService.getDauTotal(date);
        //
        Double orderAmountTotal = publisherService.getOrderAmountTotal(date);
        //创建一个list存储结果数据
        ArrayList<Map> result = new ArrayList<>();
        //创建Map来存储KV传入list
        HashMap<String, Object> dauMap = new HashMap<>();
        //给map赋值
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);
        //创建另一个Map给新增设备赋值
        HashMap<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id","dau");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        //创建另一个Map给新增设备赋值
        HashMap<String, Object> orderMap = new HashMap<>();
        orderMap.put("id","order_amount");
        orderMap.put("name","新增交易额");
        orderMap.put("value",orderAmountTotal);
        //注意顺序
        result.add(dauMap);
        result.add(newMidMap);
        result.add(orderMap);

        //返回结果
        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id") String id,
                                     @RequestParam("date") String date) {
        HashMap<String, Map> hashMap = new HashMap<>();

        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        if ("dau".equals(id)) {
            Map map = publisherService.getHourTotal(date);

            Map yesterdayMap = publisherService.getHourTotal(yesterday);

            hashMap.put("yesterday", yesterdayMap);
            hashMap.put("today", map);
        }else if("order_amount".equals(id)){
            Map map = publisherService.getOrderAmountHourMap(date);
            Map yesterdayMap = publisherService.getOrderAmountHourMap(yesterday);
            hashMap.put("yesterday", yesterdayMap);
            hashMap.put("today", map);

        }

        return JSONObject.toJSONString(hashMap);

    }
}
