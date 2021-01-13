package com.ctrip.rhd;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class WordCountKafka {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);  //写死并行度

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop200:9092");
        properties.setProperty("auto.offset.reset", "latest");
        //从kafka读取数据
        DataStreamSource<String> outPut = env.addSource(new FlinkKafkaConsumer011<String>("fisb", new SimpleStringSchema(), properties));

        outPut.print();
        //执行任务
        env.execute(); //默认并行度，当前电脑核数
    }
}
