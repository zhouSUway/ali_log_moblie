package com.mobile.etl.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMapper extends Mapper<IntWritable,Text,Text,IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private Text keyWord = new Text();

    @Override
    protected void map(IntWritable key, Text values, Context context) throws IOException, InterruptedException {
        String value = values.toString();
        String[] splited = value.split("\\^A");

        String ip = splited[0];
        String time = splited[1];
        String userAgent = splited[2];
        String urlPath = splited[3];


        StringTokenizer str = new StringTokenizer(ip + " " + time + " " + userAgent + " " + urlPath);
        while (str.hasMoreTokens()) {
            keyWord.set(str.nextToken());
            context.write(keyWord, one);
        }
    }
}
