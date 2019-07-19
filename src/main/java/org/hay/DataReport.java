package org.hay;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.hay.function.MyAggFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.*;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.OutputTag;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.hay.myWatermark.myWatermark;
import org.hay.source.MyRedisSource;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.text.ParseException;

import java.text.SimpleDateFormat;
import java.util.*;

public class DataReport {

    private static Logger logger=LoggerFactory.getLogger(DataReport.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //使用EventTime作为参考时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //checkpoint配置
        env.setMaxParallelism(1);
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置statebackend

        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop001/flink/checkpoints",true));


        /**
         *
         * kafkaSource
         * topic hospitalLog
         * 数据格式 {"dt":"2019-07-05 10:04:20","loc_area":"01","name":"张三","dis_type":"s1","sex":1,........}
         */
        String topic="hospitalLog";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","172.19.45.95:9092,172.19.45.93:9092,172.19.45.94:9092");
        prop.setProperty("group.id","hospitalLog_group");

        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);
        DataStream<String> data = env.addSource(myConsumer);


        //取出Redis数据做地区映射
        DataStream<HashMap<String, String>> mapdata = env.addSource(new MyRedisSource()).broadcast();

        DataStream<String> resdata = data.connect(mapdata).flatMap(new CoFlatMapFunction<String, HashMap<String, String>, String>() {

            //存储地区的映射关系
            private HashMap<String, String> allmap = new HashMap<String, String>();

            public void flatMap1(String value, Collector<String> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);

//                String dt = jsonObject.getString("dt");
                String areaCode = jsonObject.getString("loc_area");
                //System.out.println("这条的地区的代号为："+areaCode);

                //获取映射地区
                String area = allmap.get(areaCode);
                System.out.println("这条的是：" + area);
                jsonObject.put("loc_area",area);
                out.collect(jsonObject.toJSONString());
                }

            //flapmap2处理redis数据
            public void flatMap2(HashMap<String, String> value, Collector<String> out) throws Exception {
                this.allmap = value;
            }
        });




        DataStream<Tuple4<Long, String, String,String>> mapData = resdata.map(new MapFunction<String, Tuple4<Long, String, String,String>>() {
            @Override
            public Tuple4<Long, String,String, String> map(String line) throws Exception {

                JSONObject jsonObject = JSONObject.parseObject(line);
                String dt = jsonObject.getString("dt");
                long time = 0;


                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyy-MM-dd HH:mm:ss");
                    Date parse = sdf.parse(dt);
                    time = parse.getTime();
                    //System.out.println("时间："+time);
                } catch (ParseException e) {
                    logger.error("分析异常" + dt, e.getCause());
                }

                String dis_type = jsonObject.getString("dis_type");
                String loc_area = jsonObject.getString("loc_area");
                String sex = jsonObject.getString("sex");

                return new Tuple4<Long, String, String,String>(time, dis_type, loc_area,sex);
            }
        });


        //去除异常的数据
        DataStream<Tuple4<Long, String, String,String>> filterData = mapData.filter(new FilterFunction<Tuple4<Long, String, String,String>>() {
            @Override
            public boolean filter(Tuple4<Long, String, String,String> value) throws Exception {
                boolean flag = true;
                if (value.f0 == 0) {
                    flag = false;
                }
                return flag;
            }
        });


        //保存迟到的数据
        OutputTag<Tuple4<Long, String, String ,String>> outputTag = new OutputTag<Tuple4<Long, String, String,String>>("late-data"){};

        SingleOutputStreamOperator<Tuple4<String, String, String, String>> resultData = filterData.assignTimestampsAndWatermarks(new myWatermark())
                .keyBy(1, 2, 3 ).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(3))
                .sideOutputLateData(outputTag)
                .apply(new MyAggFunction());



        DataStream<Tuple4<Long, String, String,String>> sideOutput = resultData.getSideOutput(outputTag);

        //迟到数据存储另一个topic
        String outtopic="lateLog";

        Properties outprop=new Properties();
        outprop.setProperty("bootstrap.servers","172.19.45.95:9092,172.19.45.93:9092,172.19.45.94:9092");
        outprop.setProperty("zookeeper.connect", "172.19.45.95:2181,172.19.45.95:2181,172.19.45.95:2181");
        outprop.setProperty("transaction.timeout.ms",60000*15+"");

        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(outtopic, new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), outprop, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);

        sideOutput.map(new MapFunction<Tuple4<Long, String ,String,String>, String>() {
            @Override
            public String map(Tuple4<Long, String, String,String> value) throws Exception {
                return value.f0+"\t"+value.f1+"\t"+value.f2+"\t"+value.f3;
            }
        }).addSink(myProducer);




        /**
         * 计算结果存在es中
         * */
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("172.19.45.93", 9200, "http"));

        ElasticsearchSink.Builder<Tuple4<String, String, String, String>> esSinkBuilder = new ElasticsearchSink.Builder<Tuple4<String, String, String, String>>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple4<String, String, String, String>>() {
                    public IndexRequest createIndexRequest(Tuple4<String, String, String, String> element) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("time", element.f0);
                        json.put("dis_type", element.f1);
                        json.put("loca_area", element.f2);
                        json.put("sex", element.f3);

                        String id = element.f0.replace(" ", "_") +
                                "-" + element.f1 + "-" + element.f2;

                        return Requests.indexRequest()
                                .index("registerindex")
                                .type("registertype")
                                .id(id)
                                .source(json);
                    }

                    @Override
                    public void process(Tuple4<String, String, String, String> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        //设置批量写数据到es缓冲区大小
        esSinkBuilder.setBulkFlushMaxActions(1);

        resultData.addSink(esSinkBuilder.build());

        env.execute("RegisterReport");

    }
}
