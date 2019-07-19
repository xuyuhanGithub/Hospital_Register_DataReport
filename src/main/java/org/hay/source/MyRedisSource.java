package org.hay.source;


import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;


/**
 * redis数据初始化
 * hset loc_areas 京口区 01,02,03
 * hset loc_areas 大港区 04,05
 * hhet loc_areas 润州区 06,07,08,09
 * hhet loc_areas 丹徒区 12,14,15
 * hhet loc_areas 新区   17,20,22
 * .......................
 * redis保存地区和编号映射的信息
 * 把地区和编号映射的信息组装
 */

public class MyRedisSource implements SourceFunction<HashMap<String, String>> {

    private Logger logger = LoggerFactory.getLogger(MyRedisSource.class);

    private final long SLEEP_MILLION = 60000;
    private boolean Running = true;
    private Jedis jedis = null;

    public void run(SourceContext<HashMap<String, String>> sourceContext) throws Exception {

        this.jedis = new Jedis("172.19.45.95", 6379);
        HashMap<String, String> keyValuemap = new HashMap<String, String>();
        while (Running) {
            try {
                keyValuemap.clear();
                Map<String, String> areas = jedis.hgetAll("loc_areas");
                for (Map.Entry<String, String> entry : areas.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] splits = value.split(",");
                    for (String split : splits) {
                        keyValuemap.put(split, key);
                    }
                }
                if (keyValuemap.size() > 0) {
                    sourceContext.collect(keyValuemap);
                } else {
                    logger.warn("Empty Data in Redis!");
                }
                Thread.sleep(SLEEP_MILLION);
            } catch (JedisConnectionException e) {
                logger.error("redis连接异常，重新连接", e.getCause());
                jedis = new Jedis("localhost", 6379);
            } catch (Exception e) {
                logger.error("source 异常", e.getCause());
            }

        }
    }


    public void cancel() {
        Running = false;
        if (jedis != null) {
            jedis.close();
        }

    }
}
