package com.alibaba.rocketmq.storm.model;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.rocketmq.storm.util.TransactionUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

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

    private boolean isFull;//log信息是否是完整的

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

    /*

        '$request_time-_-$remote_addr-_-$host-_-$upstream_addr-_-$upstream_status-_-$time_local-_-$request-_-$status-_-$body_bytes_sent-_-$http_referer-_
        -$http_user_agent-_-$http_x_forwarded_for-_-$upstream_response_time'

        0.174-_-186.2.136.129-_-global.ymtracking.com-_-10.1.10.11:8080-_-302-_-23/Sep/2015:06:05:02 +0000-_-GET
        /trace?offer_id=104259&aff_id=13468&aff_sub=13894046979 HTTP/1.1-_-302-_-541-_---_-Mozilla/5.0 (Linux; U; Android 4.2.2; es-es; Bmobile_AX620
        Build/JDQ39) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30-_---_-0.174

        0.002-_-54.251.117.21-_-global.ymtracking.com-_-10.2.10.11:8080-_-200-_-23/Sep/2015:04:00:22 +0000-_-GET
        /conv?transaction_id=3f82ba27a-82c8-428f-b4437824c9fd81c817173f929f55ee090c585297d440014&source=mobvista HTTP/1.1-_-200-_-84-_---_---_---_-0.002

     */
    public AccessLog(String logInfo) {

        if ( null == logInfo || logInfo.length() == 0 ) {
            this.isFull = false;
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
            if ( this.request.contains(KEY_CLICK) ) {
                this.isClick = true;
                this.isFull = true;
            } else if ( this.request.contains(KEY_CONV) ) {
                this.isClick = false;
                this.isFull = true;
            } else {
                this.isFull = false;
            }

        }

    }

    public String offerId() {

        String offId = null;
        if ( this.isFull && isClick ) {
            offId = getRequestParamKV().getString("offer_id");//aff_id
        } else if ( this.isFull && !isClick ) {
            String tranId = getRequestParamKV().getString("transaction_id");
            long[] result = TransactionUtil.decode(tranId);
            if ( null != result && result.length == 5 ) {
                offId = String.valueOf(result[4]);
            }
        }
        return StringUtils.defaultIfBlank(offId, "0");
    }

    public String affiliateId() {

        String affId = null;
        if ( this.isFull && this.isClick ) {
            affId = getRequestParamKV().getString("aff_id");//aff_id
        } else if ( this.isFull && !isClick ) {
            String tranId = getRequestParamKV().getString("transaction_id");
            long[] result = TransactionUtil.decode(tranId);
            if ( null != result && result.length == 5 ) {
                affId = String.valueOf(result[3]);
            }
        }
        return StringUtils.defaultIfBlank(affId, "0");
    }

    public static String parseReginFromUpstreamAddr(String upstreamAddr) {

        if ( null != upstreamAddr && upstreamAddr.split("\\.").length == 4 ) {//10.2.10.11:8080
            String[] addrs = upstreamAddr.split("\\.");
            if ( NumberUtils.isDigits(addrs[0]) && NumberUtils.isDigits(addrs[1]) ) {
                return addrs[0] + "." + addrs[1];
            }
        }
        return "unknown";
    }

    //trace?offer_id  =  108674  &  aff_id  =  7720  &  aff_sub4=d06H27I76B6HOV8NGRV62A9O
    private JSONObject getRequestParamKV() {

        if ( this.request == null || this.request.length() == 0 ) {
            return null;
        }
        String[] arr = this.request.split("\\?|&");
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

    public static void main(String[] args) {

        System.out.println(SIMPLE_DATE_FORMAT.format(new Date()));
        System.out.println(getDate("21/Sep/2015:08:00:02 +0000"));
        System.out.println(parseReginFromUpstreamAddr("10.2.10.11:8080"));
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
