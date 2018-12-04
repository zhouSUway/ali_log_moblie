package com.mobile.etl.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class LocalToHDFS {

            public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

                args = new String[]{"C:\\Users\\Administrator\\IdeaProjects\\bigdata3\\Log_Mobile\\src\\main\\java\\data\\20170531.log",
                        "hdfs://192.168.243.130:9000/logs/output1"};
                Configuration conf = new Configuration();

                conf.set("fs.defaultFS", "hdfs://192.168.243.130:9000");

                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                if (otherArgs.length < 2) {
                    System.out.println("USage:wordCount<in><out>");
                    System.exit(2);
                }

                //创新job并且名字
                Job job = Job.getInstance(conf, "LocalToHDFS");
                //1.设置job运行的类
                job.setJarByClass(LocalToHDFS.class);

                job.setMapperClass(MyMapper.class);
                job.setReducerClass(MyReducer.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);

                boolean isSuccess = job.waitForCompletion(true);

                System.out.println(isSuccess ? 0 : 1);


            }
        }