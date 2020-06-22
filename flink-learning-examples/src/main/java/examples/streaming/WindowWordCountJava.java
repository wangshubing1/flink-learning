package examples.streaming;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author: king
 * @Date: 2020-06-15
 * @Desc: TODO
 */

public class WindowWordCountJava {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        /*DataStream<Tuple2<String,Integer>> dataStream =env
                .socketTextStream("localhost",9999)
                .flatMap(new Splitter())
                .keyBy(0)
                //5S内相同的word会被累加，5s以外则另行计算
                .timeWindow(Time.seconds(5))
                .sum(1);*/
        /**
         * lambda表达式
         * 需要指定数据类型
         */
        DataStreamSource<String> stream =env.socketTextStream("localhost",9999);
        stream.flatMap((String f,Collector<Tuple2<String, Integer>> collector)->{
            for (String word:f.toLowerCase().split("\\W+")){
                collector.collect(new Tuple2<String, Integer>(word,1));
            }
        }).keyBy(0)
        .sum(1)
        .print();
        //dataStream.print();
        env.execute("Window WordCount");
    }


    public static class Splitter implements FlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word:s.split(" ")){
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
