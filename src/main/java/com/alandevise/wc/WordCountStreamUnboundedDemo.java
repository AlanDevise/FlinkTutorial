package com.alandevise.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Filename: WordCountStreamUnboundedDemo.java
 * @Package: com.alandevise.wc
 * @Version: V1.0.0
 * @Description: 1. 通过socket监听无界流
 * @Author: Alan Zhang [initiator@alandevise.com]
 * @Date: 2023年11月12日 15:35
 */

public class WordCountStreamUnboundedDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取数据：socket
        DataStreamSource<String> socketDS = env.socketTextStream("192.168.56.102", 7777);
        // 3. 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                )
                .returns(Types.TUPLE(Types.STRING,Types.INT))   // 显式指定类型，避免类型擦除
                .keyBy(value -> value.f0)
                // 分组聚合
                .sum(1);
        // 4. 输出
        sum.print();
        // 5. 执行
        env.execute();
    }
}
