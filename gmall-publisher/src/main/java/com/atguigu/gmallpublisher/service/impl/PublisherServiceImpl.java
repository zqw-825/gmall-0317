package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getHourTotal(String date) {
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        HashMap<String, Long> result = new HashMap<String, Long>();

        for (Map map : list) {
            result.put((String) map.get("LH"),(Long) map.get("CT"));
        }

        return result;
    }
}

