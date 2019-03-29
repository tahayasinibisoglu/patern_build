package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Categorized implements FlatMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {
    @Override
    public void flatMap(Tuple2<String, Double> t1, Collector<Tuple2<String, Double>> collector) throws Exception  {

        double val=0;

        if(t1.f0.equals("celsius"))
        {
            if(t1.f1>35.0){
                val+=2;
            }else if (t1.f1>26.0){
                val+=1;
            }else {
                val+=0;
            }
        }
        else if(t1.f0.equals("humidity"))
        {
            if(t1.f1>35.0){
                val+=2;
            }else if (t1.f1>25.0){
                val+=1;
            }else {
                val+=0;
            }
        }
        else if(t1.f0.equals("lux"))
        {
            if(t1.f1>450.0){
                val+=2;
            }else if (t1.f1>300.0){
                val+=1;
            }else {
                val+=0;
            }
        }
        else if(t1.f0.equals("gassensor"))
        {
            if(t1.f1>150.0){
                val+=2;
            }else if (t1.f1>90.0){
                val+=1;
            }else {
                val+=0;
            }
        }
        collector.collect(new Tuple2<String, Double>(t1.f0, val));
    }
}
