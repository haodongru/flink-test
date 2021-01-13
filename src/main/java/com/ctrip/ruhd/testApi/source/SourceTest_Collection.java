package com.ctrip.ruhd.testApi.source;


import com.ctrip.ruhd.testApi.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class SourceTest_Collection {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //传入list数据

        DataStreamSource<SensorReading> sc = env.fromCollection(Arrays.asList(new SensorReading("asldfjoj_1", 35213213514L, 35.4)
                , new SensorReading("asldfjoj_2", 35213213525L, 35.4)
                , new SensorReading("asldfjoj_3", 35213213523L, 35.4)
                , new SensorReading("asldfjoj_4", 35213213511L, 35.4)
        ));

        //输出信息
        sc.print("sc");

        //执行
        env.execute();


    }
}
