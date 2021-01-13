package com.ctrip.ruhd.testApi.source;

import com.ctrip.ruhd.testApi.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class SourceTest_UDF {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //自定义数据源
        DataStreamSource<SensorReading> sss = env.addSource(new MySensorFuntion());

        sss.print();

        env.execute();
    }

    public static class MySensorFuntion implements SourceFunction<SensorReading> {
        //定义一个标志位，是run方法一直执行
        private boolean running = true;

        //把数据包装成sensorreading类型的数据
        //自定义10个初始温度值,定义一个生成随机数的对象

        Random rd = new Random();


        //override
        public void run(SourceContext<SensorReading> ctx) throws Exception {

            //定义一个集合，存放温度和id

            HashMap<String, Double> hm = new HashMap<String, Double>();

            for (int i = 1; i < 10; i++) {
                hm.put("sensor_" + (i + 1), rd.nextGaussian() * 20 + 60);
            }

            //创建循环
            while (running) {
                for (String key : hm.keySet()) {
                    Double newrd = hm.get(key) + rd.nextGaussian();
                    ctx.collect(new SensorReading(key, System.currentTimeMillis(), newrd));
                }
                //输出时间间隔
                Thread.sleep(3000L);
            }


        }

        public void cancel() {
            running = false;
        }
    }
}
