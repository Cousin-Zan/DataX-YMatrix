package com.alibaba.datax.plugin.reader.influxdb2httpreader;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Tooi
 * @date 2020/8/14 16:39
 * @description
 */
public class Constant {
    static final String DEFAULT_DATA_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String TIME_SPECIFY_KEY = "_time";

    static final Set<String> MUST_CONTAINED_SPECIFY_KEYS = new HashSet<>();
    static {
        // 必须包含 '_time' 字段
        MUST_CONTAINED_SPECIFY_KEYS.add(TIME_SPECIFY_KEY);
    }
}
