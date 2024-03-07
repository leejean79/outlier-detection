package com.leejean.withKafka;

import com.mysql.jdbc.PreparedStatement;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;

public class MyMySqlSink {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDS = env.readTextFile("src/main/input/sensor-data.log");

        inputDS.addSink(
                new RichSinkFunction<String>() {

                    private Connection conn = null;
                    private PreparedStatement pstmt = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "");
                        pstmt = (PreparedStatement) conn.prepareStatement("INSERT INTO sensor VALUES (?,?)");
                    }

                    @Override
                    public void close() throws Exception {

                        pstmt.close();
                        conn.close();
                    }

                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        String[] datas = value.split(" ");
                        pstmt.setString(1, datas[0]);
                        pstmt.setString(2, datas[1]);
                        pstmt.execute();
                    }
                }

        );

        env.execute();

    }
}
