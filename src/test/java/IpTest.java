import com.mobile.etl.util.IpUtil;
import com.mobile.etl.util.ip.IPSeeker;

/**
 * @ClassName IpTest
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 解析ip的测试类
 **/
public class IpTest {
    public static void main(String[] args) {
       //System.out.println(IPSeeker.getInstance().getCountry("183.62.92.113"));//广东省深圳市
       //System.out.println(IPSeeker.getInstance().getCountry("192.168.216.111"));//局域网
        IpUtil.RegionInfo ip = IpUtil.parserIp("192.168.216.111");
        System.out.println(ip);
    }
}