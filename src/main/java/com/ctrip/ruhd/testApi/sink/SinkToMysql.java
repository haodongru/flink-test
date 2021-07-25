package com.ctrip.ruhd.testApi.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class SinkToMysql {
    public static void main(String[] args) throws  Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","centos-03:9092");

        //添加源
        DataStreamSource<String> aFinal = env.addSource(new FlinkKafkaConsumer011<String>("first", new SimpleStringSchema(), properties));

        //把afinal转为一个二元组的datastream
        SingleOutputStreamOperator<Tuple2<String, Double>> outputStreamOperator = aFinal.map(new MapFunction<String, Tuple2<String, Double>>() {
            public Tuple2<String, Double> map(String s) throws Exception {
                return new Tuple2<String, Double>(s.split(",")[0], new Double(s.split(",")[2]));
            }
        });

        //输出到mysql
        outputStreamOperator.addSink(new MyJdbcSink());

        env.execute();

    }
}
