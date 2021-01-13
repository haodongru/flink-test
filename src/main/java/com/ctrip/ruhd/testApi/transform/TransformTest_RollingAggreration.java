package com.ctrip.ruhd.testApi.transform;

import com.ctrip.ruhd.testApi.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest_RollingAggreration {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //定义文件路径
        String inputPath = "E:\\IdeaProjects\\flink\\src\\main\\resources\\hello.txt";

        //读取文件内容，放入inputDataSet里面
        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        //把数据包装秤SensorReading
        SingleOutputStreamOperator<SensorReading> output = inputDataStream.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String value) throws Exception {
                String[] s = value.split(",");
                return new SensorReading(s[0], new Long(s[1]), new Double((s[2])));
            }
        });

//        KeyedStream<SensorReading, Tuple> srt = output.keyBy(0); //int的索引只能用在元组中，projoType不支持
        KeyedStream<SensorReading, Tuple> keyedStream = output.keyBy("id");
        //max输出最大值，其他值保留最初的，maxBy取最大值，其他保留当前行
        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.max("temperature");

        resultStream.print();

        //执行
        env.execute();
    }
}
