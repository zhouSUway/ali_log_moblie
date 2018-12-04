package qianfeng.etl.util;

import cz.mallat.uasparser.UASparser;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class UserAgentUtil {

    private static final Logger logger =  Logger.getLogger(UserAgentUtil.class);

    static UserAgentInfo info = new UserAgentInfo();

    //获取uasparser对象
    private static UASparser uaSparser = null;

    public static UserAgentInfo parseUserAgent(String userAgent){
       if (StringUtils.isEmpty(userAgent)){
           return null;
       }
       //使用uasparse获取对象代理对象
       try {
           cz.mallat.uasparser.UserAgentInfo ua = uaSparser.parse(userAgent);
       }catch (Exception e){
           logger.warn("userAgent解析异常",e);
       }

       return info;
   }



    /**
     * 封装浏览器相关属性信息
     */

    static  class UserAgentInfo{
        private String browserName;
        private String browserVersion;
        private String osName;
        private String osVersion;

        public String getBrowserName() { return browserName; }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }


        @Override
        public String toString() {
            return
                    "browserName='" + browserName + '\'' +
                    ", browserVersion='" + browserVersion + '\'' +
                    ", osName='" + osName + '\'' +
                    ", osVersion='" + osVersion + '\'' ;
        }
    }

}
