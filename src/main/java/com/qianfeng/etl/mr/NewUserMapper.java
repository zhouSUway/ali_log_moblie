package com.qianfeng.etl.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 新增用户mapper类
 */
public class NewUserMapper extends Mapper<LongWritable,Text,Text,Text> {

    /**
     * key(2018-12-03 website IE 8.0) uuid(12345)
     * key(2018-12-03 website IE 11.0) uuid(12345)
     */
//    数据key的公共维度 输出key的key公共维度 ↓
//    新增用户维度分析、地域维度分析、事件维度分析


}
