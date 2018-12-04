package com.mobile.etl.util;

import javafx.scene.chart.PieChart;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;

public class DateUtil {


    public static  String getTime(String time){

        //输入日志[01/Dec/2018:20:14:17 +0800]
        String times= String.valueOf(time);
        String substring = times.substring(0, 10);

        long l = Long.parseLong(substring);

        //目标日期格式
        SimpleDateFormat TARGET_FORMAT = new SimpleDateFormat("yyyyMMdd");
        return TARGET_FORMAT.format(l);
    }


    public static void main(String[] args) {
       String test = getTime("1496208391.276");
       System.out.println(test);
    }
}
