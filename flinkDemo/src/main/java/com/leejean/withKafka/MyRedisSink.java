package com.leejean.withKafka;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class MyRedisSink {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> socketTextStream = env.socketTextStream("localhost", 9999);

        // 向Redis中输出内容
        FlinkJedisPoolConfig jedisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();

        socketTextStream.addSink(new RedisSink<String>(jedisConfig,
                new RedisMapper<String>() {
                    @Override
                    public RedisCommandDescription getCommandDescription() {
//                        redis 最外层的key
                        return new RedisCommandDescription(RedisCommand.HSET,"sensor");
                    }

                    @Override
                    // Hash类型：这个指定的是 hash 的key
                    public String getKeyFromData(String s) {
                        String[] datas = s.split(",");
                        return datas[1];
                    }

                    @Override
                    // Hash类型：这个指定的是 hash 的 value
                    public String getValueFromData(String s) {
                        String[] datas = s.split(",");
                        return datas[2];
                    }
                }));

        env.execute();

    }
}
