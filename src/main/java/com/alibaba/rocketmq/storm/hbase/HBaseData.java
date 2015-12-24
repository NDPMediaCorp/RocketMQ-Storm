package com.alibaba.rocketmq.storm.hbase;

import java.util.Date;
import java.util.Map;

public class HBaseData {

    private String table;

    private String columnFamily;

    private String rowKey;

    private Map<String, byte[]> data;

    private Date date;

    public HBaseData() {
    }

    public HBaseData(String table, String rowKey, String columnFamily, Map<String, byte[]> data) {
        this.table = table;
        this.columnFamily = columnFamily;
        this.rowKey = rowKey;
        this.data = data;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public Map<String, byte[]> getData() {
        return data;
    }

    public void setData(Map<String, byte[]> data) {
        this.data = data;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public String toString() {
        String datas = "[=][";
        for (Map.Entry<String, byte[]> entry:data.entrySet()){
            datas = datas + entry.getKey() + "," + new String(entry.getValue()) + "]";
        }
        datas += "][=]";
        return "HBaseData{" +
                "table='" + table + '\'' +
                ", columnFamily='" + columnFamily + '\'' +
                ", rowKey='" + rowKey + '\'' +
                ", date = " + date +
                ", data=" + datas +
                '}';
    }
}
