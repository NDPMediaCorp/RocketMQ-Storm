package com.alibaba.rocketmq.storm.model;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.rocketmq.storm.util.TransactionUtil;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * AccessLog Created with mythopoet.
 *
 * @author penuel (penuel.leo@gmail.com)
 * @date 15/9/21 下午5:36
 * @desc nginx access.log
 */
public class AccessLog {

    private static final Logger LOG = LoggerFactory.getLogger(AccessLog.class);

    private String requestTime;

    private String remoteAddr;//userIp

    private String upstreamAddr;//dcIp

    private String timeLocal;//time eg. 21/Sep/2015:08:00:02 +0000

    private String request;//parse offId,affId eg. GET /trace?offer_id=108674&aff_id=7720&aff_sub4=d06H27I76B6HOV8NGRV62A9O HTTP/1.1

    private String httpUserAgent;//ua eg. Mozilla/5.0 (Linux; U; Android 2.3.6; tr-tr; GT-S5570I Build/GINGERBREAD) AppleWebKit/533.1 (KHTML,
    // like Gecko) Version/4.0 Mobile Safari/533.1

    private String upstreamResponseTime;

    /* --------------- */
    private static final String SEPARATOR = "-_-";

    private boolean isFull = false;//log信息是否是完整的

    private boolean isClick;//该request是否为click请求

    /*
    dcs = {'10.1': 'us-east(Northern Virginia)',
       '10.3': 'us-west(Northern California)',
       '10.2': 'asia-pacific(Singapore)',
       '162.13': 'euro(London)',
       '134.213': 'euro(London)',
       '10.5': 'south-america(Sao Paulo)',
       '169.57': 'SOFT-MEXICO',
       '159.122': 'SOFT-GERMANY',
       }
     */
    private String region;//10.2.10.11:8080

    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

    private static final String KEY_CLICK = "/trace?", KEY_CONV = "/conv?";

    private String offId;

    private String affId;

    /*

        '$request_time-_-$remote_addr-_-$host-_-$upstream_addr-_-$upstream_status-_-$time_local-_-$request-_-$status-_-$body_bytes_sent-_-$http_referer-_
        -$http_user_agent-_-$http_x_forwarded_for-_-$upstream_response_time'

        0.174-_-186.2.136.129-_-global.ymtracking.com-_-10.1.10.11:8080-_-302-_-23/Sep/2015:06:05:02 +0000-_-GET
        /trace?offer_id=104259&aff_id=13468&aff_sub=13894046979 HTTP/1.1-_-302-_-541-_---_-Mozilla/5.0 (Linux; U; Android 4.2.2; es-es; Bmobile_AX620
        Build/JDQ39) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30-_---_-0.174

        0.002-_-54.251.117.21-_-global.ymtracking.com-_-10.2.10.11:8080-_-200-_-23/Sep/2015:04:00:22 +0000-_-GET
        /conv?transaction_id=3f82ba27a-82c8-428f-b4437824c9fd81c817173f929f55ee090c585297d440014&source=mobvista HTTP/1.1-_-200-_-84-_---_---_---_-0.002

     */

    /*
        fluentd 发送过来的数据是
        {"message":"0.003-_-190.7.241.154-_-global.ymtracking.com-_-10.5.10.10:8080-_-302-_-28/Sep/2015:08:41:13 +0000-_-GET
        /trace?offer_id=104259&aff_id=30148&aff_sub=25625&aff_sub2=1001679&aff_sub3
        =AAAAA20u8FDF78F0vuD7wFD2F45F03824uv92_EDi_201509281638_EDi_25625_EDi_1001679_EDi_r_rDjtFyvg_EDi_Dee_ezFqh_EDi__EDi_190.7.241
        .154_EDi_XL_EDi_1D7D6v1E339vu466_EDi_4001_EDi_061FFw77-F3D5-4006-9631-20u2Fvw904wF_EDi_15 HTTP/1.1-_-302-_-541-_---_-Mozilla/5.0 (Linux; U; Android 4
        .2.2; es-us; HUAWEI Y600-U351 Build/HUAWEIY600-U351) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30-_---_-0.003",
        "time":"1443429673","tag":"lbs.lbs_vncc_access.201.294"}

        {"message":"0.007-_-200.219.198.111-_-global.ymtracking.com-_-10.5.10.11:8080-_-200-_-28/Sep/2015:08:50:26 +0000-_-GET
        /conv?transaction_id=17859d29c-1f85-787d-a2fdc1a36d0391862a2637152e1a5c935c03c5fb8360011&source=Pmovil HTTP/1.0-_-200-_-84-_---_---_---_-0.002",
        "time":"1443430226","tag":"lbs.lbs_vncc_access.201.294"}
     */
    public AccessLog(String logInfo) {

        if ( null == logInfo || logInfo.length() == 0 ) {
            this.isFull = false;
        }
        try {
            JSONObject json = JSONObject.parseObject(logInfo);
            if ( null == json || json.getString("message") == null ) {
                this.isFull = false;
                return;
            }
            logInfo = json.getString("message");
        } catch ( Exception e ) {
            LOG.error("AccessLog parse Error:logInfo=" + logInfo, e);
            this.isFull = false;
            return;
        }
        String[] logArray = logInfo.split(SEPARATOR);
        if ( null == logArray || logArray.length < 13 ) {
            this.isFull = false;
        } else {
            this.requestTime = logArray[0];
            this.remoteAddr = logArray[1];
            this.upstreamAddr = logArray[3];
            this.timeLocal = logArray[5];
            this.request = logArray[6];
            this.httpUserAgent = logArray[10];
            this.upstreamResponseTime = logArray[12];
            this.region = parseReginFromUpstreamAddr(this.upstreamAddr);
            JSONObject obj = getRequestParamKV();
            if ( this.request.contains(KEY_CLICK) ) {
                this.isClick = true;
                this.isFull = true;
            } else if ( this.request.contains(KEY_CONV) ) {
                this.isClick = false;
                this.isFull = true;
            } else {
                this.isFull = false;
            }
            this.offId = offerId(obj);
            this.affId = affiliateId(obj);

        }

    }

    private String offerId(JSONObject requestParam) {

        String offId = "0";
        if ( !this.isFull || null == requestParam ) {
            return offId;
        }
        if ( requestParam.containsKey("offer_id") ) {
            offId = requestParam.getString("offer_id");//aff_id
        } else if(requestParam.containsKey("transaction_id")){
            String tranId = requestParam.getString("transaction_id");
            long[] result = TransactionUtil.decode(tranId);
            if ( null != result && result.length == 5 ) {
                offId = String.valueOf(result[4]);
            }
        }
        if ( null == offId || offId.length() == 0 ) {
            return "0";
        }
        return offId;
    }

    private String affiliateId(JSONObject requestParam) {

        String affId = "0";
        if ( !this.isFull || null == requestParam  ) {
            return affId;
        }
        if ( requestParam.containsKey("aff_id") ) {
            affId = requestParam.getString("aff_id");//aff_id
        } else if(requestParam.containsKey("transaction_id")){
            String tranId = requestParam.getString("transaction_id");
            long[] result = TransactionUtil.decode(tranId);
            if ( null != result && result.length == 5 ) {
                affId = String.valueOf(result[3]);
            }
        }
        if ( null == affId || affId.length() == 0 ) {
            return "0";
        }
        return affId;
    }

    public static String parseReginFromUpstreamAddr(String upstreamAddr) {

        if ( null != upstreamAddr && upstreamAddr.split("\\.").length == 4 ) {//10.2.10.11:8080
            String[] addrs = upstreamAddr.split("\\.");
            if ( NumberUtils.isDigits(addrs[0]) && NumberUtils.isDigits(addrs[1]) ) {
                return String.valueOf("'" + addrs[0] + "." + addrs[1]+"'");
            }
        }
        LOG.warn("unknow region:{}"+upstreamAddr);
        return "unknown";
    }

    //trace?offer_id  =  108674  &  aff_id  =  7720  &  aff_sub4=d06H27I76B6HOV8NGRV62A9O
    private JSONObject getRequestParamKV() {

        if ( this.request == null || this.request.length() == 0 ) {
            return null;
        }
        String[] arr = this.request.split("\\?|&| ");
        JSONObject obj = new JSONObject();
        for ( String s : arr ) {
            if ( s.contains("=") ) {
                String[] kvString = s.split("=");
                if ( null != kvString && kvString.length >= 2 ) {
                    obj.put(kvString[0], kvString[1]);
                }
            }
        }
        return obj;
    }

    // 21/Sep/2015:08:00:02 +0000
    public static Date getDate(String timeLocal) {

        if ( null == timeLocal || timeLocal.length() == 0 ) {
            return new Date();
        }
        try {
            return SIMPLE_DATE_FORMAT.parse(timeLocal);
        } catch ( ParseException e ) {
            e.printStackTrace();
        }
        return null;
    }

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

    public static void main(String[] args) {
        String loginfo = "{\"message\":\"0.007-_-200.219.198.111-_-global.ymtracking.com-_-10.5.10.11:8080-_-200-_-28/Sep/2015:08:50:26 +0000-_-GET " +
                "/conv?transaction_id=17859d29c-1f85-787d-a2fdc1a36d0391862a2637152e1a5c935c03c5fb8360011&source=Pmovil HTTP/1.0-_-200-_-84-_---_---_---_-0" +
                ".002\",\"time\":\"1443430226\",\"tag\":\"lbs.lbs_vncc_access.201.294\"}";
        AccessLog log = new AccessLog(loginfo);
        System.out.println(log);
    }

    public boolean isFull() {

        return isFull;
    }

    public void setFull(boolean isFull) {

        this.isFull = isFull;
    }

    public String getRequestTime() {

        return requestTime;
    }

    public void setRequestTime(String requestTime) {

        this.requestTime = requestTime;
    }

    public String getRemoteAddr() {

        return remoteAddr;
    }

    public void setRemoteAddr(String remoteAddr) {

        this.remoteAddr = remoteAddr;
    }

    public String getUpstreamAddr() {

        return upstreamAddr;
    }

    public void setUpstreamAddr(String upstreamAddr) {

        this.upstreamAddr = upstreamAddr;
    }

    public String getTimeLocal() {

        return timeLocal;
    }

    public void setTimeLocal(String timeLocal) {

        this.timeLocal = timeLocal;
    }

    public String getRequest() {

        return request;
    }

    public void setRequest(String request) {

        this.request = request;
    }

    public String getHttpUserAgent() {

        return httpUserAgent;
    }

    public void setHttpUserAgent(String httpUserAgent) {

        this.httpUserAgent = httpUserAgent;
    }

    public String getUpstreamResponseTime() {

        return upstreamResponseTime;
    }

    public void setUpstreamResponseTime(String upstreamResponseTime) {

        this.upstreamResponseTime = upstreamResponseTime;
    }

    public boolean isClick() {

        return isClick;
    }

    public void setClick(boolean isClick) {

        this.isClick = isClick;
    }

    public String getRegion() {

        return region;
    }

    public void setRegion(String region) {

        this.region = region;
    }
}
