package com.atguigu.utils;

/**
 * @author zqw
 * @create 2020-08-16 10:34
 */
import java.util.Random;

public class RandomNum {
    public static int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }
}
