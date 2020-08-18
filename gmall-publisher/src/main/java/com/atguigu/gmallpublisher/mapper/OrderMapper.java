package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author zqw
 * @create 2020-08-18 20:29
 */
public interface OrderMapper {

    Double selectOrderAmountTotal(String date);

    List<Map> selectOrderAmountHourMap(String date);

}
