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

    private boolean isClick;

//    private double avgResponseTime; // 暂时不需要

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

    public boolean isClick() {

        return isClick;
    }

    public void setClick(boolean isClick) {

        this.isClick = isClick;
    }
}
