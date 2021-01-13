package com.ctrip.ruhd.testApi.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformTest_Base {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //定义文件路径
        String inputPath = "E:\\IdeaProjects\\flink\\src\\main\\resources\\hello.txt";

        //读取文件内容，放入inputDataSet里面
        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        //针对文件内容，试验transform基本方法。
        //1.map方法

        SingleOutputStreamOperator<Integer> mapStream = inputDataStream.map(new MapFunction<String, Integer>() {
            public Integer map(String value) throws Exception {
                return value.length();
            }
        });

        //flatmap方法

        SingleOutputStreamOperator<String> flatMap = inputDataStream.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] flat = value.split(",");
                for (String s : flat) {
                    out.collect(s);
                }
            }
        });

        //filter方法
        SingleOutputStreamOperator<String> filter = inputDataStream.filter(new FilterFunction<String>() {
            public boolean filter(String value) throws Exception {
                //过滤只取sensor_1开头的数据
                return value.startsWith("sensor_1");
            }
        });

        mapStream.print("map");
        flatMap.print("flatmap");
        filter.print("filter");

        env.execute();


    }
}
