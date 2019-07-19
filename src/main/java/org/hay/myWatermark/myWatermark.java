package org.hay.myWatermark;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class myWatermark implements AssignerWithPeriodicWatermarks<Tuple4<Long, String, String,String>> {

    Long currentMaxTimesstamp=0L;
    final Long maxOutOfOrderness=3000L;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimesstamp-maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple4<Long, String, String,String> element, long previousElementTimestamp) {
        Long timestamp = element.f0;
        currentMaxTimesstamp=Math.max(timestamp,currentMaxTimesstamp);
        return timestamp;
    }
}
