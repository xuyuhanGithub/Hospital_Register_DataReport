package org.hay.function;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class MyAggFunction implements WindowFunction<Tuple4<Long, String, String,String>, Tuple4<String, String, String, String>, Tuple, TimeWindow>{
    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<Long, String, String,String>> input, Collector<Tuple4<String, String, String, String>> out) throws Exception {
        String dis_type = tuple.getFieldNotNull(0).toString();
        String loc_area = tuple.getFieldNotNull(1).toString();
        String sex= tuple.getFieldNotNull(2).toString();

        Iterator<Tuple4<Long, String, String,String>> it = input.iterator();

        ArrayList<Long> arrayList = new ArrayList<>();
        long count=0;
        while(it.hasNext()) {
            Tuple4<Long, String, String,String> next = it.next();
            arrayList.add(next.f0);
            count+=1;

        }

        Collections.sort(arrayList);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time = sdf.format(arrayList.get(arrayList.size() - 1));
        Tuple4<String, String, String, String> res = new Tuple4<>(time, loc_area, dis_type, sex);

        out.collect(res);


    }}
