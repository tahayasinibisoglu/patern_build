package org.apache.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class Fp_tree  extends ProcessAllWindowFunction<
        String,
        String,
        TimeWindow> {

    ArrayList<String> buffa;

    public Fp_tree(){
        ArrayList<String> data_list = new ArrayList<String>();
        this.buffa=data_list;
    }

    public void process(Context context,
                        Iterable<String> IN,
                        Collector<String> collector) throws Exception {
        for (String in : IN) {
            buffa.add(in);
        }

        new FPGrowth().FPAlgo(new FPGrowth().readFile(buffa),null);
        buffa.removeAll(buffa);
    }
}
