package qianfeng.etl.util;


import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import qianfeng.etl.util.ip.IPSeeker;

/**
 * ip的解析工具类
 */
public class IpUtil extends IPSeeker {

    private static final Logger logger = Logger.getLogger(IpUtil.class);

    static RegionInfo info = new RegionInfo();

    /**
     * ip
     * 返回地域信息
     */

    public  static RegionInfo pasrserIp(){
        return null;
    }
    public static RegionInfo parserIp(String ip){
        //判断是否为空

        if(StringUtils.isEmpty(ip)||StringUtils.isEmpty(ip.trim())){
            return info;
        }


        try {

            //ip不为空 正常解析
            String country = info.country;

            if("局域网".equals(country)){
                info.setCountry("中国");
                info.setProvince("北京");
                info.setCity("海淀区");
            }else if (country!=null||StringUtils.isNotEmpty(country.trim())){

                //查找省的位置
                info.setCountry("中国");
                int index = country.indexOf("省");
                if(index >0){
                    //设置省份
                    info.setProvince(country.substring(0,index+1));
                    //判断是否有市
                    int index2= country.indexOf("市");
                    if (index2>0){
                        //设置市
                        info.setCity(country.substring(index+1,index2+1));
                    }else {

                        //代码剔除到省之后，剩下直辖市，自治区，特区
                        String flage = country.substring(0,2);

                        switch (flage){
                            case "内蒙":
                                info.setProvince("内蒙古");
                                country = country.substring(3);
                                index = country.indexOf("市");
                                if(index >0){
                                    //设置市
                                    info.setCity(country.substring(0,index+1));
                                }
                                break;
                            case "广西":
                            case "西藏":
                            case "新疆":
                            case "宁夏":
                                info.setProvince(flage);
                                country = country.substring(2);
                                index  = country.indexOf("市");
                                if(index >0){
                                    //设置市
                                    info.setCity(country.substring(0,index+1));
                                }
                                break;
                            case "北京":
                            case "上海":
                            case "重庆":
                            case "天津":
                                info.setProvince(flage+"市");
                                country = country.substring(3);
                                index = country.indexOf("区");
                                if(index >0){
                                    char ch = country.charAt(index-1);
                                    if(ch !='小'&& ch !='校'&& ch != '军'){
                                        info.setCity(country.substring(0,index+1));
                                    }
                                }
                                //在直辖市中如果有县级的
                                index = country.indexOf('县');
                                if(index>0){
                                    info.setCity(country.substring(0,index+1));
                                }
                                break;
                            case "香港":
                            case "澳门":
                            case "台湾":
                                info.setProvince(flage+"特别行政区");
                                break;
                            default:
                                    break;


                        }
                    }
                }
            }

        }catch (Exception e){

            logger.warn("解析ip工具方法异常",e);
        }
        return info;
    }
    /**
     * 使用该类进行封装地域信息，ip解析出来的国家、省、市
     *
     */

    public static class RegionInfo{

        private final String DEFAULT_VALUE = "unknow";
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
                    "country='" + country + '\'' +
                    ", province='" + province + '\'' +
                    ", city='" + city + '\'' +
                    '}';
        }
    }

}
