package org.apache.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class ItemSet extends ProcessAllWindowFunction<
        Tuple2<String, Double>,
        String,
        TimeWindow> {

    public void process(Context context,
                        Iterable<Tuple2<String, Double>> IN,
                        Collector<String> collector) throws Exception
    {
        String to_collect = "";
        for (Tuple2<String, Double> in : IN) {
            String additional="";
            if (in.f0.equals("celsius")){
                additional = "c:"+(String) toString(in.f1);}
            else if (in.f0.equals("humidity")){
                additional = "h:"+(String) toString(in.f1);}
            else if (in.f0.equals("lux")){
                additional = "l:"+(String) toString(in.f1);}
            else if (in.f0.equals("gassensor")){
                additional = "g:"+(String) toString(in.f1);
            }
            else if (in.f0.equals("sharp")){
                additional = "s:"+(String) toString(in.f1);
            }
            else if (in.f0.equals("pir")) {
                additional = "p:" + (String) toString(in.f1);
            }
            to_collect += additional + ",";

        }
        String to_colletor = to_collect.substring(0,to_collect.length()-1);
        collector.collect(to_colletor);
    }

    private String toString(Double f1) {
        String s = String.format("%.0f", f1);
        return s ;
    }

}
