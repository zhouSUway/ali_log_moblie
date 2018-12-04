package com.mobile.etl.util;

import com.github.jarod.qqwry.IPZone;
import com.github.jarod.qqwry.QQWry;

import java.io.IOException;

public class Ip2region {

    //获取省份
    public static void getProvince(String ip){
        try {
            QQWry qqWry = new QQWry();
            IPZone zone = qqWry.findIP(ip);
            System.out.println(zone.getProvince());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //获取城市
    public static void getCity(String ip){
        try {
            QQWry qqWry= new QQWry();
            IPZone zone = qqWry.findIP(ip);
            System.out.println(zone.getSubInfo());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        try {
            getProvince("128.344.222.54");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
