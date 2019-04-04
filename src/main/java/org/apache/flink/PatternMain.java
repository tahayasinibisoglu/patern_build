package org.apache.flink;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.util.List;
import java.util.Map;

public class PatternMain {
    public static void main(String[] args) throws Exception {

        // Argument Check

        if (args.length != 0) {
            System.out.println("No arguments pls");
            return;
        }

        // Initial Settings

        String hostName = "127.0.0.1";
        Integer port = 9000;

        System.out.println("IAQ Stream Processing Pipeline Starting V1.0 mg");
        System.out.println("Host Set To : " + hostName);
        System.out.println("Listening Port : " + port.toString());

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // Data Stream Creation

        DataStream<String> stream_input = environment.socketTextStream(hostName, port);

        // Data Stream Processing Pipeline Starts !!!

        // FlatMap      : DataStream    ->  DataStream      , Split String By Space
        // Split        : DataStream    ->  SplitStream     , Split Stream By Key

        SplitStream<Tuple2<String, Double>> split_stream = stream_input
                .flatMap(new SplitByColon())
                .split(new SplitStreamBk());


        DataStream<Tuple2<String, Double>> stream_data_windowed = split_stream
                .keyBy(0)
                .timeWindow(Time.seconds(12))
                .reduce(new SumReducerFn())
                .flatMap(new Categorized());


        DataStream<String> stream_itemset_ham = stream_data_windowed
              .flatMap(new ItemSetHam());
        stream_itemset_ham.print();

        DataStream<String> stream_itemset = stream_data_windowed.
                keyBy(0).
                timeWindowAll(Time.seconds(20)).
                process(new ItemSet());





        Pattern<String,?> pattern = Pattern.<String>begin("start").where(
                new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.equals("h:1");
                    }
                }
        ).or(new SimpleCondition<String>() {
                 @Override
                 public boolean filter(String value) throws Exception {
                     return value.equals("l:1");
                 }
             }

        ).followedBy("middle").where(
                        new SimpleCondition<String>() {
                            @Override
                            public boolean filter(String value) throws Exception {
                                return value.equals("l:1");
                            }
                        }
                ).followedBy("end").where(
                        new SimpleCondition<String>() {
                            @Override
                            public boolean filter(String value) throws Exception {
                                return value.equals("g:0");
                            }
                        }
                );

        PatternStream<String> patternStream = CEP.pattern(stream_itemset_ham,pattern);

        DataStream<String> result = patternStream.select(
                new PatternSelectFunction<String, String>() {
                    @Override
                    public String select(Map<String, List<String>> map) throws Exception {
                        System.out.println("Found");
                        return "Found:"+
                                map.get("start")+"->"+
                                map.get("middle")+"->"+
                                map.get("end");
                    }
                }

        );
        DataStream<String> stream_Fp_tree = stream_itemset.
                timeWindowAll(Time.seconds(150)).
                process(new Fp_tree());



        environment.execute();

    }
}
