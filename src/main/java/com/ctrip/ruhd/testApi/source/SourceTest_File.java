package com.ctrip.ruhd.testApi.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest_File {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //添加数据源
        DataStreamSource<String> dss = env.readTextFile("E:\\IdeaProjects\\flink\\src\\main\\resources\\Sensor.txt");

        //打印
        dss.print();

        //执行
        env.execute();
    }
}
