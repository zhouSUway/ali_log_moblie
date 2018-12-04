package qianfeng.etl.util;

import com.github.jarod.qqwry.IPZone;
import com.github.jarod.qqwry.QQWry;

public class IpRegion {

    public void getIp(String ip){
        try {
            QQWry qqwry = new QQWry();
            IPZone zone = qqwry.findIP(ip);
            System.out.println(zone.getProvince());
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}

