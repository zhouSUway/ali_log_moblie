package com.mobile.etl.util;

import java.util.HashMap;
import java.util.Map;

public class UrlPathUtil {

    public static void getUrlPath(String url){

        Map<String,String> map = new HashMap<String, String>();

        String[] u1=url.split("&");
        for(int i=0;i<u1.length;i++){
            String[] s1=u1[i].split("=");
            map.put(s1[0],s1[1]);
        }
        System.out.print(map);
    }


    public static void main(String[] args) {
        getUrlPath("BCImg.gif?en=e_pv&p_url=http%3A%2F%2Fwww.beicai.com%2Fworld%2F&p_ref=http");
    }
}
