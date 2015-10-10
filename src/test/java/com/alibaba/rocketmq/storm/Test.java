package com.alibaba.rocketmq.storm;

import com.alibaba.rocketmq.storm.hbase.Helper;
import com.alibaba.rocketmq.storm.model.AccessLog;
import com.alibaba.rocketmq.storm.model.AggregationResult;
import com.alibaba.rocketmq.storm.model.HBaseData;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test Created with mythopoet.
 *
 * @author penuel (penuel.leo@gmail.com)
 * @date 15/9/30 上午10:40
 * @desc
 */
public class Test {

    public static final String COLUMN_NCLICK = "nclick";

    public static final String COLUMN_NCONV = "nconv";

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    public static void main(String[] args) {
        AccessLog accessLog = new AccessLog(
                "{\"message\":\"0.003-_-190.7.241.154-_-global.ymtracking.com-_-10.5.10.10:8080-_-302-_-28/Sep/2015:08:41:13 +0000-_-GET\n"
                        + "        /trace?offer_id=104259&aff_id=30148&aff_sub=25625&aff_sub2=1001679&aff_sub3\n" + "        " +
                        "=AAAAA20u8FDF78F0vuD7wFD2F45F03824uv92_EDi_201509281638_EDi_25625_EDi_1001679_EDi_r_rDjtFyvg_EDi_Dee_ezFqh_EDi__EDi_190.7.241\n"
                        + "        .154_EDi_XL_EDi_1D7D6v1E339vu466_EDi_4001_EDi_061FFw77-F3D5-4006-9631-20u2Fvw904wF_EDi_15 " +
                        "HTTP/1.1-_-302-_-541-_---_-Mozilla/5.0 (Linux; U; Android 4\n"
                        + "        .2.2; es-us; HUAWEI Y600-U351 Build/HUAWEIY600-U351) AppleWebKit/534.30 (KHTML, " +
                        "like Gecko) Version/4.0 Mobile Safari/534.30-_---_-0.003\",\n"
                        + "        \"time\":\"1443429673\",\"tag\":\"lbs.lbs_vncc_access.201.294\"}" +
                        "");

        if ( !accessLog.isFull() ) {
            return;
        }
        HashMap<String, HashMap<String, AggregationResult>> parserAggResultMap = new HashMap<>();
        String offerId = accessLog.getOffId();
        String affiliateId = accessLog.getAffId();
        String mapKey = Helper.generateKeyForHBase(offerId, affiliateId);
        HashMap<String, AggregationResult> regionResultMap1;
        String regionMapKey = accessLog.isClick() ? COLUMN_NCLICK + "@" + accessLog.getRegion() : COLUMN_NCONV + "@" + accessLog.getRegion();
        AggregationResult aggregationResult;
        if ( !parserAggResultMap.containsKey(mapKey) ) {
            regionResultMap1 = new HashMap<>();
            aggregationResult = new AggregationResult();
            aggregationResult.setTotalCount(1);
            aggregationResult.setOffId(accessLog.getOffId());
            aggregationResult.setAffId(accessLog.getAffId());
            aggregationResult.setClick(accessLog.isClick());
        } else {
            regionResultMap1 = parserAggResultMap.get(mapKey);
            if ( !regionResultMap1.containsKey(regionMapKey) ) {
                aggregationResult = new AggregationResult();
                aggregationResult.setTotalCount(1);
                aggregationResult.setOffId(accessLog.getOffId());
                aggregationResult.setAffId(accessLog.getAffId());
                aggregationResult.setClick(accessLog.isClick());
            } else {
                aggregationResult = regionResultMap1.get(regionMapKey);
                aggregationResult.setTotalCount(aggregationResult.getTotalCount() + 1);
            }
        }
        regionResultMap1.put(regionMapKey, aggregationResult);
        parserAggResultMap.put(mapKey, regionResultMap1);



        if ( null == parserAggResultMap || parserAggResultMap.isEmpty() ) {
        }
        List<HBaseData> hBaseDataList = new ArrayList<>();
        Map<String, String> redisCacheMap = new HashMap<>();
        HashMap<String, AggregationResult> regionResultMap;
        AggregationResult result;
        for ( Map.Entry<String, HashMap<String, AggregationResult>> entry : parserAggResultMap.entrySet() ) {
            regionResultMap = entry.getValue();
            String rowKey = entry.getKey();
            StringBuilder nclick = new StringBuilder();
            StringBuilder nconv = new StringBuilder();
            Map<String, byte[]> data = new HashMap<>();

            for ( Map.Entry<String, AggregationResult> regionMapEntry : regionResultMap.entrySet() ) {
                result = regionMapEntry.getValue();
                String[] region = regionMapEntry.getKey().split("@");
                if ( null == region || region.length <= 1 ){
                    continue;
                }
                if ( COLUMN_NCLICK.equals(region[0]) ) {
                    nclick.append(", ").append(region[1]).append(": ").append(result.getTotalCount());
                } else {
                    nconv.append(", ").append(region[1]).append(": ").append(result.getTotalCount());
                }
            }

            String nclickResult = nclick.toString();
            if ( nclickResult.contains(",") ) {
                nclickResult = nclickResult.substring(1);
            }
            nclickResult = "{" + nclickResult + "}";
            String nconvResult = nconv.toString();
            if ( nconvResult.contains(",") ) {
                nconvResult = nconvResult.substring(1);
            }
            nconvResult = "{" + nconvResult + "}";


            data.put(COLUMN_NCLICK, nclickResult.getBytes(DEFAULT_CHARSET));
            data.put(COLUMN_NCONV, nconvResult.getBytes(DEFAULT_CHARSET));
            redisCacheMap.put(rowKey, "{connection: " + nclickResult + ", request: " + nconvResult + "}");
        }



    }

}
