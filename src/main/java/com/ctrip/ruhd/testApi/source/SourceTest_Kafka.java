package com.ctrip.ruhd.testApi.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class SourceTest_Kafka {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","centos-03:9092");

        //添加源
        DataStreamSource<String> aFinal = env.addSource(new FlinkKafkaConsumer011<String>("final", new SimpleStringSchema(), properties));


        //打印
        aFinal.print();


        //执行
        env.execute();
    }
}
