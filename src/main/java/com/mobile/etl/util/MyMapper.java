package com.mobile.etl.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMapper extends Mapper<Object,Text,Text,IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private Text keyWord = new Text();

    @Override
    protected void map(Object key, Text values, Context context) throws IOException, InterruptedException {
        String value = values.toString();
        StringTokenizer str = new StringTokenizer(value);
        while (str.hasMoreTokens()) {
            keyWord.set(str.nextToken());
            context.write(keyWord,one);
        }
    }
}
