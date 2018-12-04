import com.mobile.etl.util.IpUtil;
import com.mobile.etl.util.ip.IPSeeker;

import java.util.List;

/**
 * @ClassName IpTest
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 解析ip的测试类
 **/
public class IpTest {
    public static void main(String[] args) throws Exception{
       //System.out.println(IPSeeker.getInstance().getCountry("183.62.92.113"));//广东省深圳市
       //System.out.println(IPSeeker.getInstance().getCountry("192.168.216.111"));//局域网
//        IpUtil.RegionInfo ip = IpUtil.parserIp("192.168.216.111");
//        System.out.println(ip);

      List<String> ips =  IPSeeker.getInstance().getAllIp();
        for(String ip :ips){
            System.out.println(IpUtil.parserIp(ip));
        }
    }
}