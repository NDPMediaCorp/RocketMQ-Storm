package com.alibaba.rocketmq.storm.model;

/**
 * ParserAggregationResult Created with mythopoet.
 *
 * @author penuel (penuel.leo@gmail.com)
 * @date 15/9/21 下午5:23
 * @desc
 */
public class AggregationResult {

    private String offId;

    private String affId;

    private long totalCount;

    private double avgResponseTime;

    public String getOffId() {
        return offId;
    }

    public void setOffId(String offId) {
        this.offId = offId;
    }

    public String getAffId() {
        return affId;
    }

    public void setAffId(String affId) {
        this.affId = affId;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public double getAvgResponseTime() {
        return avgResponseTime;
    }

    public void setAvgResponseTime(double avgResponseTime) {
        this.avgResponseTime = avgResponseTime;
    }
}
