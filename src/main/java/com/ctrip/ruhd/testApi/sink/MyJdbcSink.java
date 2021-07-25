package com.ctrip.ruhd.testApi.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MyJdbcSink extends RichSinkFunction<Tuple2<String,Double>> {

    //创建连接
    Connection connection = null;
    //创建预编译参数，设置预编译语句
    PreparedStatement insertStatement = null;
    PreparedStatement updateStatement = null;


    @Override
    public void open(Configuration parameters) throws Exception {
        //设置连接信息
        connection = DriverManager.getConnection("jdbc:mysql://centos-03:3306/test","root","rhd@1234");

        //设置预编译语句
        insertStatement = connection.prepareStatement("insert into sensor_reading (id,temp) values (?,?)");
        updateStatement = connection.prepareStatement("update sensor_reading set temp = ? where id=?");


    }

    @Override
    public void close() throws Exception {
        updateStatement.close();
        insertStatement.close();
        connection.close();
    }

    //override
    public void invoke(Tuple2<String,Double> value, Context context) throws Exception {
        updateStatement.setDouble(1,value.f1);
        updateStatement.setString(2,value.f0);
        updateStatement.execute();
        if(updateStatement.getUpdateCount() == 0){
            insertStatement.setString(1,value.f0);
            insertStatement.setDouble(2,value.f1);
            insertStatement.execute();
        }


    }
}
