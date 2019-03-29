package org.apache.flink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class SumReducerFn implements ReduceFunction<Tuple2<String, Double>> {
    @Override
    public Tuple2<String, Double> reduce(Tuple2<String, Double> t1, Tuple2<String, Double> t2) {
        double avg = t1.f0.equals("sharp") || t1.f0.equals("pir") ? t1.f1 : (t1.f1 + t2.f1) / 2;
        return new Tuple2<String, Double>(t1.f0, avg);

    }
}
