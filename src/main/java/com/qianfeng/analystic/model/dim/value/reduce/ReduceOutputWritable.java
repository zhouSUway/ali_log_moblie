package com.qianfeng.analystic.model.dim.value.reduce;

import com.qianfeng.analystic.model.dim.value.OutputValueBaseWritable;
import com.qianfeng.common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReduceOutputWritable extends OutputValueBaseWritable {

    private MapWritable value= new MapWritable();//k--v形式
    private KpiType kpi;//必须reduce输出后要有指标，可需要kpi来找对应的sql


    @Override
    public KpiType getKpi() {
        return getKpi();
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        this.value.write(dataOutput);
        WritableUtils.writeEnum(dataOutput,this.kpi);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

       this.value.readFields(dataInput);
        WritableUtils.readEnum(dataInput,KpiType.class);//枚举的反序列化

    }



}
