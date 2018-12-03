package com.mobile.etl.util;

import com.mobile.etl.util.ip.IPSeeker;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * @ClassName IpUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description //TODO $
 **/
public class IpUtil {
    private static final Logger logger = Logger.getLogger(IpUtil.class);
    static RegionInfo info = null;
    /**
     * 解析ip的方法
     * @param ip 被解析的ip
     * @return
     */

    public static RegionInfo parserIp(String ip){
        if(StringUtils.isEmpty(ip)){
            return info;
        }
        //ip是一个正常值
        // 获取国家信息
        String country = IPSeeker.getInstance().getCountry(ip);

        if(StringUtils.isNotEmpty(country)){
            info = new RegionInfo();
            //判断country是否为局域网
            if(country.equals("局域网")){
                info.setCountry("中国");
                info.setProvince("北京市");
                info.setCity("昌平区");
            } else if(country.equals("全球")) {
                info.setCountry("太平洋");
                info.setProvince("胡赛岛");
                info.setCity("胡赛野人");
            }else if(country.contains("省")||country.contains("市")){
                //返回的字段中是否包括市
                //返回字段中是否包括省这个字，否则判断是前两个字符是否为北京、天津、上海、重庆和广西、内蒙、西藏、新疆、宁夏和香港、澳门、台湾
                String province = country.substring(0,1);
                if(StringUtils.isNotEmpty(province)){

                    if(province.contains("北京")){
                        info.setCity("北京市");
                    }else if(province.contains("上海")){
                        info.setCity("上海市");
                    }else if(province.contains("重庆")){
                        info.setCity("重庆市");
                    }else if(province.contains("天津")){
                        info.setCity("天津");
                    }
                    else if(province.contains("广西")){
                        info.setProvince("广西壮族自治区");
                    }else if(province.contains("宁夏")){
                        info.setProvince("宁夏回族自治区");
                    }else if(province.contains("新疆")){
                        info.setProvince("新疆维族自治区");
                    }else if(province.contains("内蒙")){
                        info.setProvince("内蒙古蒙古自治区");
                    }else if(province.contains("西藏")){
                        info.setProvince("西藏藏族自治区");
                    }else if(province.contains("香港")){
                        info.setCity("香港特别行政区");
                    }else if(province.contains("澳门")){
                        info.setProvince("澳门特别行政区");
                    }else if(province.contains("台湾")){
                        info.setProvince("台湾省");
                    }else {
                        System.out.println("有误");
                    }

                }

            }
        }


        return null;
    }


    /**
     * 封装ip解析出来的信息
     */
    public static class RegionInfo{
        private String DEFAULT_VALUE = "unknown";
        private String country = DEFAULT_VALUE;
        private String province = DEFAULT_VALUE;
        private String city = DEFAULT_VALUE;

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        @Override
        public String toString() {
            return "RegionInfo{" +
                    country + '\t' + province + '\t' + city + '\t' + '}';
        }
    }
}