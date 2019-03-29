package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class SplitByColon implements FlatMapFunction<String, Tuple2<String, Double>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Double>> collector) throws Exception {
        if (s.length() > 2) {
            String[] subs = s.split(":");
            if (subs[1].equals("sharp")) {
                double val;
                if (subs[3].equals("in")) {
                    val = 1;
                } else
                    val = -1;
                collector.collect(new Tuple2<String, Double>(subs[1], val));
            } else
                collector.collect(new Tuple2<String, Double>(subs[1], Double.valueOf(subs[3])));
        }
    }
}
