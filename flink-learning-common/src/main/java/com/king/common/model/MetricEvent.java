package com.king.common.model;

import java.util.Map;

/**
 * @Author: king
 * @Date: 2019-07-31
 * @Desc: TODO
 */

public class MetricEvent {
    /**
     * Metric name
     */
    private String name;

    /**
     * Metric timestamp
     */
    private Long timestamp;

    /**
     * Metric fields
     */
    private Map<String, Object> fields;

    /**
     * Metric tags
     */
    private Map<String, String> tags;
}
