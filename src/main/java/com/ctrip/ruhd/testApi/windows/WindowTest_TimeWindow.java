package com.ctrip.ruhd.testApi.windows;

import com.ctrip.ruhd.testApi.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 测试窗口分配器及窗口函数
 */
public class WindowTest_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

/*        //定义文件路径
        String inputPath = "E:\\IdeaProjects\\flink\\src\\main\\resources\\hello.txt";*/

        //读取文件内容，放入inputDataStream里面
        DataStream<String> inputDataStream = env.socketTextStream("centos-03",7777);

        //把读取到的数据封装成sensorReading类型

        DataStream<SensorReading> mapStream = inputDataStream.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String value) throws Exception {
                String[] sensor = value.split(",");
                return new SensorReading(sensor[0], new Long(sensor[1]), new Double(sensor[2]));

            }
        });

        SingleOutputStreamOperator<Integer> resultStream = mapStream.keyBy("id")

                //时间滚动窗口，滑动窗口传2个参数
                .timeWindow(Time.seconds(10))
                //计数窗口，滑动同理
//                .countWindow(15)
                //时间会话窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(30)))

                //窗口函数
//                .min("temperature");
                //reduceFunction，和以前一样
/*                .reduce(new ReduceFunction<SensorReading>() {
                    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                        return null;
                    }
                })*/
                //创建一个计数的增量窗口函数
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {

                    //创建一个累加器，给初始值
                    public Integer createAccumulator() {
                        return 0;
                    }

                    //累加计数
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    //获取结果
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });


                //全量窗口函数
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> resultStream2 = mapStream.keyBy("id")
                .timeWindow(Time.seconds(15))

                //apply窗口，创建一个窗口函数，返回一个三元组，第一个是id，第二个是窗口时间，第三个是计数值
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        //获取key，id值
                        String s = tuple.getField(0);
                        Long end = window.getEnd();

                        //把迭代器中的数据转为list
                        Integer i = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<String, Long, Integer>(s, end, i));
                    }
                });

        //测试滑动窗口
        SingleOutputStreamOperator<Double> myAggregateResuleStream = mapStream.keyBy("id")
                .countWindow(10, 3)
                .aggregate(new AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>() {
                    public Tuple2<Double, Integer> createAccumulator() {
                        //返回tuple2类型的对象
                        return new Tuple2<Double, Integer>(0.0, 0);
                    }

                    public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
                        return new Tuple2<Double, Integer>(createAccumulator().f0 + value.getTemperature(), createAccumulator().f1 + 1);
                    }

                    public Double getResult(Tuple2<Double, Integer> accumulator) {
                        return accumulator.f0 / accumulator.f1;
                    }

                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                        return new Tuple2<Double, Integer>(a.f0 + b.f0, a.f1 + b.f1);
                    }
                });

//        myAggregateResuleStream.print();
        resultStream2.print();
//        resultStream.print("resultStream:");



        env.execute();
    }
}
