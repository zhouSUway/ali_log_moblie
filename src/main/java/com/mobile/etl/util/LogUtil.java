package com.mobile.etl.util;

import java.util.Map;

/**
 * @ClassName LogUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 将hdfs中的数据的每一行都进行ip、useragent、时间戳等的解析
 **/
public class LogUtil {


    /**
     *
     * @param log  一行日志
     * @return 将解析后的k-v存储到map中，以便etl的mapper进行使用
     */
    public static Map parserLog(String log){

        String splited[] = log.split("\\^A");
        String ip = splited[0];
        String time = splited[1];
        String userAgent=splited[2];
        String url=splited[3];
        try {

        }catch (Exception e){
            e.printStackTrace();
        }

            return null;
    }
}