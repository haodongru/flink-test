package com.ctrip.rhd;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

// 批处理wordcount
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //定义文件路径
        String inputPath = "E:\\IdeaProjects\\flink\\src\\main\\resources\\hello.txt";

        //读取文件内容，放入inputDataSet里面
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        //调用flatmap方法，按空格把数据拆开，转完成（word,1）二元组进行统计
        DataSet<Tuple2<String,Integer>> outPut = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)  //传入对应位置,按照第一个位置进行分组
                .sum(1);  //将第二个位置上的数据求和

        outPut.print();


    }
    //自定义myflatmap类,实现FlatMapFunction接口
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //按空格分词
            String[] words = value.split(" ");
            for(String word: words){
                out.collect(new Tuple2<String,Integer>(word,1));
            }
        }
    }
}
