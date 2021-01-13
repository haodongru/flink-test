package com.ctrip.rhd;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);  //写死并行度
/*        //定义文件路径
        String inputPath = "E:\\IdeaProjects\\flink\\src\\main\\resources\\hello.txt";

        //读取文件内容，放入inputDataSet里面
        DataStream<String> inputDataStream = env.readTextFile(inputPath);*/

        //用flink自带的parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        //从socket文本流读取数据
        DataStreamSource<String> inputDataStream = env.socketTextStream(host, port);

        //调用flatmap方法，按空格把数据拆开，转完成（word,1）二元组进行统计
        DataStream<Tuple2<String,Integer>> outPut = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)  //传入对应位置,按照第一个位置进行重分区，hash
                .sum(1);  //将第二个位置上的数据求和

        outPut.print();
        //执行任务
        env.execute(); //默认并行度，当前电脑核数
    }
}
