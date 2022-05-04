package DWD;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.MyKafkaUtils;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建source
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtils.getConsumer("ods_base_log");
        env.setParallelism(1);
        DataStreamSource<String> mainStream = env.addSource(kafkaConsumer);

        //操作数据 业务模块
        //去除脏数据
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonData = mainStream.process(new ProcessFunction<String, JSONObject>() {

            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject parseObject = JSON.parseObject(s);
                    collector.collect(parseObject);
                } catch (Exception e) {
                    context.output(dirtyTag, s);
                }
            }
        });
        DataStream<String> dirtyData = jsonData.getSideOutput(dirtyTag);
        dirtyData.print("dirtyData  - - - ");

        //去除重复访问用户
        SingleOutputStreamOperator<JSONObject> jsonCData = jsonData.keyBy(data -> data.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    ValueState<String> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<String>("isExist", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        if ("1".equals(isNew)) {
                            if (state == null) {
                                state.update("1");
                            } else {
                                jsonObject.getJSONObject("common").put("is_new", "0");
                            }
                        }
                        return jsonObject;
                    }
                });

        //进行分流
        OutputTag<String> startTag = new OutputTag<String>("page") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("dispaly") {
        };

        SingleOutputStreamOperator<String> pageData = jsonCData.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                JSONObject start = jsonObject.getJSONObject("start");
                if (start != null && start.size() > 0) {
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    collector.collect(jsonObject.toJSONString());
                    JSONArray displays = jsonObject.getJSONArray("displays");

                    if (displays != null) {
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);

                            //添加页面id
                            display.put("page_id", pageId);

                            //将输出写出到曝光侧输出流
                            context.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });
        DataStream<String> startData = pageData.getSideOutput(startTag);
        DataStream<String> displayData = pageData.getSideOutput(displayTag);

        startData.print("start");
        pageData.print("page");
        displayData.print("display");

        startData.addSink(MyKafkaUtils.getProducer("dwd_start_log"));
        displayData.addSink(MyKafkaUtils.getProducer("dwd_display_log"));
        pageData.addSink(MyKafkaUtils.getProducer("dwd_page_log"));


        env.execute("BaseLogApp");
    }
}
