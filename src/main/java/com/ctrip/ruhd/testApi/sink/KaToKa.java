package com.ctrip.ruhd.testApi.sink;

import com.ctrip.ruhd.testApi.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

public class KaToKa {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop200:9092");
        //从kafka读取数据
        DataStreamSource<String> outPut = env.addSource(new FlinkKafkaConsumer011<String>("fisb", new SimpleStringSchema(), properties));



        //把返回封装成sensorReading类
        SingleOutputStreamOperator<String> output = outPut.map(new MapFunction<String, String>() {
            public String map(String value) throws Exception {
                //把string类型的数据转为SensorReading
                String[] s = value.split(",");
                return new SensorReading(s[0], new Long(s[1]), new Double(s[2])).toString();

            }
        });

        //sink输出 方法
        output.addSink(new FlinkKafkaProducer011<String>("hadoop200:9092", "sb", new SimpleStringSchema()));



        env.execute();
    }
}
