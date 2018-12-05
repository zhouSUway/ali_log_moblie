package com.qianfeng.analystic.model.dim.value.map;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * map阶段输出value类型（用户模型、浏览器模型）
 */

public class TimeOutputWritable extends StatsBaseOutputWritable {
    private String id;//泛指uid\mid\sid
    private long time;//传递时间戳，应用统计session的时间长


    @Override
    public KpiTypeEnum getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.id);
        dataOutput.writeLong(this.time);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.time= dataInput.readLong();
    }


    @Override
    public int compareTo(Object o) {
        if(this==0) {
            return 0;
        }
        return 0;
    }
}
