package com.alibaba.rocketmq.storm.model;

import com.alibaba.fastjson.JSONObject;

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

    private static final String SEPARATOR = "-_-";

    private boolean isFull;//log信息是否是完整的

    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

    /*

        '$request_time-_-$remote_addr-_-$host-_-$upstream_addr-_-$upstream_status-_-$time_local-_-$request-_-$status-_-$body_bytes_sent-_-$http_referer-_
        -$http_user_agent-_-$http_x_forwarded_for-_-$upstream_response_time'

        0.005-_-88.229.248.243-_-global.ymtracking.com-_-10.11.10.10:8080-_-302-_-21/Sep/2015:08:00:02 +0000-_-GET
        /trace?offer_id=108674&aff_id=7720&aff_sub4=d06H27I76B6HOV8NGRV62A9O HTTP/1.1-_-302-_-341-_-http://9mybb.redirectvoluum
        .com:80/redirect?target=http%3A%2F%2Fglobal.ymtracking
        .com%2Ftrace%3Foffer_id%3D108674%26aff_id%3D7720%26aff_sub4%3Dd06H27I76B6HOV8NGRV62A9O&ts=1442822401230&hash=ZlOnGWQmKn7Y
        %2F6HIxCyh8KAk1tgN25Cr4ZIxNH29uiw%3D&rm=DJ-_-Mozilla/5.0 (Linux; U; Android 2.3.6; tr-tr; GT-S5570I Build/GINGERBREAD) AppleWebKit/533.1 (KHTML,
        like Gecko) Version/4.0 Mobile Safari/533.1-_---_-0.005

     */
    public AccessLog(String logInfo) {

        if ( null == logInfo || logInfo.length() == 0 ) {
            this.isFull = false;
        }
        String[] logArray = logInfo.split(SEPARATOR);
        if ( null == logArray || logArray.length < 13 ) {
            this.isFull = false;
        } else {
            this.isFull = true;
            this.requestTime = logArray[0];
            this.remoteAddr = logArray[1];
            this.upstreamAddr = logArray[3];
            this.timeLocal = logArray[5];
            this.request = logArray[6];
            this.httpUserAgent = logArray[10];
            this.upstreamResponseTime = logArray[12];
        }

    }

    //trace?offer_id  =  108674  &  aff_id  =  7720  &  aff_sub4=d06H27I76B6HOV8NGRV62A9O
    public static String getOffOrAffId(String request, boolean isOffer) {

        if ( request == null || request.length() == 0 ) {
            return null;
        }
        String[] arr = request.split("\\?|&");
        JSONObject obj = new JSONObject();
        for ( String s : arr ) {
            if ( s.contains("=") ) {
                String[] kvString = s.split("=");
                if ( null != kvString && kvString.length >= 2 ) {
                    obj.put(kvString[0], kvString[1]);
                }
            }
        }

        if ( isOffer ) {
            return obj.getString("offer_id");
        } else {
            return obj.getString("aff_id");
        }
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
}
