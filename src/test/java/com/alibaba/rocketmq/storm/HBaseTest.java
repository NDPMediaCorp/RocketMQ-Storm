package com.alibaba.rocketmq.storm;

import com.alibaba.rocketmq.storm.hbase.HBaseClient;
import com.alibaba.rocketmq.storm.hbase.exception.HBasePersistenceException;
import com.alibaba.rocketmq.storm.hbase.HBaseData;
import org.junit.Test;

import java.util.Calendar;
import java.util.List;

public class HBaseTest {

    @Test
    public void scan(){
        HBaseClient hBaseClient = new HBaseClient();
        hBaseClient.start();
        Calendar old = Calendar.getInstance();
        old.add(Calendar.DAY_OF_MONTH, -3);
        try {
            List<HBaseData> list = hBaseClient.scan("3", "9", old, Calendar.getInstance(), "eagle_log", "t");
            for (HBaseData hBaseData:list){
                System.out.println(hBaseData.toString());
            }
        } catch (HBasePersistenceException e) {
            e.printStackTrace();
        }
    }
}
