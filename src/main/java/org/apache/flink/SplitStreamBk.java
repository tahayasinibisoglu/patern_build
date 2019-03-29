package org.apache.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

import java.util.ArrayList;
import java.util.List;

public class SplitStreamBk implements OutputSelector<Tuple2<String,Double>> {

    @Override
    public Iterable<String> select(Tuple2<String, Double> in) {
        List<String> out = new ArrayList<>();

        if (in.f0.equals("celsius"))
            out.add("celsius");
        else if (in.f0.equals("humidity"))
            out.add("humidity");
        else if (in.f0.equals("lux"))
            out.add("lux");
        else if (in.f0.equals("gassensor"))
            out.add("gassensor");
        else if (in.f0.equals("sharp"))
            out.add("sharp");
        else if (in.f0.equals("pir"))
            out.add("pir");
        else
            out.add("garbage");

        return out;
    }
}
