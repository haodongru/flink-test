package com.ctrip.ruhd.testApi.transform;

import com.ctrip.ruhd.testApi.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

public class TransformTest_Reduce {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //定义文件路径
        String inputPath = "E:\\IdeaProjects\\flink\\src\\main\\resources\\hello.txt";

        //读取文件
        DataStreamSource<String> inputStream = env.readTextFile(inputPath);

        //把返回封装成sensorReading类
        SingleOutputStreamOperator<SensorReading> output = inputStream.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String value) throws Exception {
                //把string类型的数据转为SensorReading
                String[] s = value.split(",");
                return new SensorReading(s[0], new Long(s[1]), new Double(s[2]));

            }
        });

        //调用keyby 方法
        KeyedStream<SensorReading, Tuple> resultStream = output.keyBy("id");
        //调用reduce方法
        SingleOutputStreamOperator<SensorReading> result = resultStream.reduce(new ReduceFunction<SensorReading>() {
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
            }
        });

        result.print();

/*        //分流操作 split、select

        SplitStream<SensorReading> result = output.split(new OutputSelector<SensorReading>() {
            public Iterable<String> select(SensorReading value) {
                //给流盖戳
                return (value.getTemperature() > 50) ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> high = result.select("high");
        DataStream<SensorReading> low = result.select("low");
        DataStream<SensorReading> all = result.select("high", "low");

        high.print("high");
        low.print("low");
        all.print("all");*/

        env.execute();


    }
}
