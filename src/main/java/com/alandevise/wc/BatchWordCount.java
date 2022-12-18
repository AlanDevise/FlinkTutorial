package com.alandevise.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Filename: BatchWordCount.java
 * @Package: com.alandevise.wc
 * @Version: V1.0.0
 * @Description: 1. 以数据集的形式进行分析
 * @Author: Alan Zhang [initiator@alandevise.com]
 * @Date: 2022年12月04日 14:00
 */

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 从文件中读取数据
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");
        // 3. 将每行数据进行分词，然后转换成二元组类型 flatMap(扁平映射)
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // 将一行文本以空格拆分
            String[] words = line.split(" ");
            // 将每个分词转换成二元组输出
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4. 按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);
        // 5. 分组内进行聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);
        // 6. 打印输出
        sum.print();
    }
}
