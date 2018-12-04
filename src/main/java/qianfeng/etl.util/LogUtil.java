package qianfeng.etl.util;

import qianfeng.common.EventLogsConstant;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Auther:
 * @Date:
 * @Description:将采集的一行一行的日志进行解析成key-value，便于存储。
 */
public class LogUtil {
    private static final Logger logger = Logger.getLogger(LogUtil.class);
    /**
     * 192.168.216.111^A1532576375.965^A192.168.216.111^A
     * /index.html?ver=1.0&u_mid=123&en=e_cr&c_time=1532576375614&
     * ip:192.168.216.111
     * s_time:1532576375.965
     * ver:1.0
     *
     * @param log
     * @return
     */
    public static Map parserLog(String log){
        if(StringUtils.isEmpty(log)){

            return null;
        }

        //log不为空
        String fileds[] = log.split(EventLogsConstant.COLUMN_SEPARTOR);
        //fileds的长度是4

        Map<String,String> info = new ConcurrentHashMap<String,String>();
        if(fileds.length==4){

            info.put(EventLogsConstant.EVENT_COLUMN_NAME_IP,fileds[0]);
            info.put(EventLogsConstant.EVENT_COLUMN_NAME_SERVER_TIME,fileds[1].replace("\\.",""));

            //解析requestBody（）
            hadleRequestBody(fileds[3],info);
            //处理ip的解析
            hadleIp(info);
            //处理userAgent的解析
            hadleUserAgent(info);

        }
        return info;

    }

    /**
     * 将解析后的userAgent存储到info中
     * @param info
     */
    private static void hadleUserAgent(Map<String, String> info) {

        if(!info.isEmpty()){
            String userAgent = info.get(EventLogsConstant.EVENT_COLUMN_NAME_USERAGENT);
            //调用useragent的解析方法

            UserAgentUtil.UserAgentInfo uinfo = UserAgentUtil.parserUserAgent(userAgent);

            info.put(EventLogsConstant.EVENT_COLUMN_NAME_BROWSER_NAME,uinfo.getBrowserName());
            info.put(EventLogsConstant.EVENT_COLUMN_NAME_BROWSER_VERSION,uinfo.getBrowserVersion());
            info.put(EventLogsConstant.EVENT_COLUMN_NAME_OS_NAME,uinfo.getOsName());
            info.put(EventLogsConstant.EVENT_COLUMN_NAME_OS_VERSION,uinfo.getOsVersion());

        }
    }

    /**
     * 将ip解析的字段进行解析
     * @param info
     */
    private static void hadleIp(Map<String,String> info) {

        if(!info.isEmpty()){
            //获取ip
            String ip = info.get(EventLogsConstant.EVENT_COLUMN_NAME_IP);
            //调用ip的解析方法
            IpUtil.RegionInfo info1 = IpUtil.parserIp(ip);

           info.put(EventLogsConstant.EVENT_COLUMN_NAME_COUNTRY,info1.getCountry());
           info.put(EventLogsConstant.EVENT_COLUMN_NAME_PROVINCE,info1.getProvince());
           info.put(EventLogsConstant.EVENT_COLUMN_NAME_CITY,info1.getCity());

        }

    }

    /**
     * 解决请求参数
     * @param filed
     * @param info
     */

    private static void hadleRequestBody(String filed, Map<String,String> info) {


        if (StringUtils.isNotEmpty(filed)){
            //获取定位
            int index = filed.indexOf("?");
            if(index >0){
                String body = filed.substring(index+1);
                String [] kvs = body.split("&");

                for (String kv : kvs){

                    //System.out.println(kv);

                    String kv1[] = kv.split("=");
                    //System.out.println(kv1[0]);
                    //System.out.println(kv1[1]);
                    String k = kv1[0];
                    String v = null;
                    try {
                        v = URLDecoder.decode(kv1[1],"utf-8");

                    } catch (Exception e) {

                       logger.warn("values解码异常",e);

                    }

                    //判断key为空过滤
                    if(k!=null&&k.trim().equals("")){
                        //将key-value 存储到map中
                        info.put(k,v);
                       // System.out.println(k);
                    }

                }
            }
        }
    }
}
