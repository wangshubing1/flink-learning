package examples.streaming.waterMark;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author: king
 * @Date: 2020-06-18
 * @Desc: TODO
 */

/**
 * 程序详解：
 * 1、接收socket数据
 * 2、将每行数据按照逗号分隔，每行数据调用map转换成tuple<String,Long>类型。
 * 其中tuple中的第一个元素代表具体的数据，第二行代表数据的eventtime
 * 3、抽取timestamp，生成watermar，允许的最大乱序时间是10s，并打印（key,eventtime,currentMaxTimestamp,watermark）等信息
 * 4、分组聚合，window窗口大小为3秒，输出（key，窗口内元素个数，窗口内最早元素的时间，窗口内最晚元素的时间，窗口自身开始时间，窗口自身结束时间）
 */

/**
 * input:
 * 0001,1592446500000
 * 0001,1592446501000
 * (...)
 */
public class StreamingWindowWatermark {
    private static final Logger log = LoggerFactory.getLogger(StreamingWindowWatermark.class);

    public static void main(String[] args) throws Exception {
        int port = 9999;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置使用EventTime，默认processtime 每隔200ms
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置并行度为1，默认是当前机器的cpu数量
        env.setParallelism(1);
        DataStreamSource<String> stream = env.socketTextStream("localhost", port, "\n");

        DataStream<Tuple2<String, Long>> inputMap = stream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] arr = s.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });
        /**
         * 1、对数据进行timestamp提取，即调用assignTimestampsAndWatermarks函数;
         *  实例化BoundedOutOfOrdernessTimestampExtractor，重写extractTimestamp方法
         * 2、设置使用事件时间，因为WaterMark是基于事件时间
         * 3、定义时间窗口：翻滚窗口（TumblingEventWindows）、滑动窗口（timeWindow）
         *
         * 抽取timestamp和生成watermark
         */
        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            Long currenMaxTimestamp = 0L;
            final Long maxOutOfOrderness = 10000L;//最大允许的乱序时间10S
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            /**
             * 定义生成watermark的逻辑，比当前最大时间戳晚10s
             *
             * @return 比对结果
             */
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currenMaxTimestamp - maxOutOfOrderness);
            }

            //定义如何提取timestamp
            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long l) {
                //定义timestamp怎么从数据中抽取出来
                long timestamp = element.f1;
                currenMaxTimestamp = Math.max(timestamp, currenMaxTimestamp);
                //设置多并行度时获取线程id
                long id = Thread.currentThread().getId();
                String result = "extractTimestamp=======>" + ",currentThreadId:" + id + ",key:" + element.f0 + ",eventtime:[" + element.f1 + "|"
                        + sdf.format(element.f1) + "]," + "currentMaxTimestamp:[" + currenMaxTimestamp + "|" + sdf.format(currenMaxTimestamp)
                        + "],watermark:[" + getCurrentWatermark().getTimestamp() + "|" + sdf.format(getCurrentWatermark().getTimestamp()) + "]";
                System.out.println(result);
                return timestamp;
            }
        });

        DataStream<String> window = waterMarkStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))//按照消息的EventTime分配窗口，和调用TimeWindow效果一样
                .allowedLateness(Time.seconds(2))//允许数据迟到2s
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    //对window内的数据进行排序，保证数据的顺序
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
                        String key = tuple.toString();
                        List<Long> arrarList = new ArrayList<Long>();
                        Iterator<Tuple2<String, Long>> it = iterable.iterator();
                        while (it.hasNext()) {
                            Tuple2<String, Long> next = it.next();
                            arrarList.add(next.f1);
                        }
                        Collections.sort(arrarList);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = key + "," + arrarList.size() + "," + sdf.format(arrarList.get(0)) + "," + sdf.format(arrarList.get(arrarList.size() - 1))
                                + "," + sdf.format(window.getStart()) + "," + sdf.format(window.getEnd());
                        collector.collect(result);
                    }
                });
        window.print();
        env.execute("eventtime-watermark");
    }
}
