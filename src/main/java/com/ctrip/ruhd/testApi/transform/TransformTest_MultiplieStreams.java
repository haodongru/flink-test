package com.ctrip.ruhd.testApi.transform;

/**
 * 把hello.txt作为输入，取温度35为界，输出两条流。把两条流的最大值输出，及对应时间
 *
 *
 * */

import com.ctrip.ruhd.testApi.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

public class TransformTest_MultiplieStreams {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建路径
        String inputPath = "E:\\IdeaProjects\\flink\\src\\main\\resources\\hello.txt";

        final DataStreamSource<String> stringDataStreamSource = env.readTextFile(inputPath);

        //把返回结果包装成sendor_reading类
        SingleOutputStreamOperator<SensorReading> map = stringDataStreamSource.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String s) throws Exception {
                String[] ss = s.split(",");
                return new SensorReading(ss[0], new Long(ss[1]), new Double(ss[2]));
            }
        });

        //split select
        SplitStream<SensorReading> split = map.split(new OutputSelector<SensorReading>() {
            //Iterable 是一个集合，可以使用多个参数切分
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getTemperature() > 50 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });
        DataStream<SensorReading> high = split.select("high");
        DataStream<SensorReading> low = split.select("low");

        high.print("high");
        low.print("low");
        
        //把高温流转换为二元组，然后和低温流合并，然后打印出高温报警，低温正常

        SingleOutputStreamOperator<Tuple2<String, Double>> highMap = high.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<String, Double>(sensorReading.getId(), sensorReading.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = highMap.connect(low);
        SingleOutputStreamOperator<Object> streamOperator = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            public Object map1(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return new Tuple3<String, Double, String>(stringDoubleTuple2.f0, stringDoubleTuple2.f1, "high temp warning");
            }

            public Object map2(SensorReading sensorReading) throws Exception {
                return new Tuple2<Double, String>(sensorReading.getTemperature(), "normal");
            }
        });
        streamOperator.print();

        //union 合多条流，流的类型必须相同
        DataStream<SensorReading> unionStream = high.union(map,low);
        unionStream.print("union:");

        env.execute();

    }
}
