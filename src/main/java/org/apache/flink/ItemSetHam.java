package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class ItemSetHam implements FlatMapFunction<Tuple2<String, Double>,String > {
    @Override
    public void flatMap(Tuple2<String, Double> IN, Collector<String> collector) throws Exception
    {

        String additional="";

            if (IN.f0.equals("celsius")){
                additional = "c:"+(String) toString(IN.f1);}
            else if (IN.f0.equals("humidity")){
                additional = "h:"+(String) toString(IN.f1);}
            else if (IN.f0.equals("lux")){
                additional = "l:"+(String) toString(IN.f1);}
            else if (IN.f0.equals("gassensor")){
                additional = "g:"+(String) toString(IN.f1);
            }
            else if (IN.f0.equals("sharp")){
                additional = "s:"+(String) toString(IN.f1);
            }
            else if (IN.f0.equals("pir")) {
                additional = "p:" + (String) toString(IN.f1);
            }

            collector.collect(additional);
//        }

    }

    private String toString(Double f1) {
        String s = String.format("%.0f", f1);
        return s ;
    }

}

