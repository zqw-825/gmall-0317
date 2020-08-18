package com.atguigu.gmallpublisher.service;

import java.util.Map;

/**
 * @author zqw
 * @create 2020-08-17 15:44
 */
public interface PublisherService {
      public Integer getDauTotal(String date);
      public Map getHourTotal(String date);
      public Double getOrderAmountTotal(String date);
      public Map getOrderAmountHourMap(String date);
}
