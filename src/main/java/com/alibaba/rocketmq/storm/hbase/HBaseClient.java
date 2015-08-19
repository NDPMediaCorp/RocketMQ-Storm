package com.alibaba.rocketmq.storm.hbase;

import com.alibaba.rocketmq.storm.hbase.exception.HBaseDataInvalidException;
import com.alibaba.rocketmq.storm.hbase.exception.HBasePersistenceException;
import com.alibaba.rocketmq.storm.model.HBaseData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.security.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseClient {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);

    public static String KEYTAB_CONF="hbase.keytab.filename";
    public static String KEYTAB_USER="hbase.client.kerberos.principal";
    public static int ELEVEN_MINUTES=60000;
    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    private static final String DATE_FORMAT = "yyyyMMddHHmmss";

    private Configuration config;

    private volatile HBaseClientStatus status = HBaseClientStatus.JUST_CREATED;

    public void runLoginAndRenewalThread(final Configuration conf) throws IOException {

        // Do a blocking login first
        SecurityUtil.login(conf, KEYTAB_CONF, KEYTAB_USER);

        // Spawn the relogin thread next
        Thread reloginThread = new Thread() {
            @Override
            public void run() {
                while (HBaseClientStatus.STARTING == status || HBaseClientStatus.STARTED == status) {
                    try {
                        SecurityUtil.login(conf, KEYTAB_CONF, KEYTAB_USER);
                        Thread.sleep(ELEVEN_MINUTES);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                        interrupt();
                    }
                }
            }
        };
        reloginThread.setDaemon(true);
        reloginThread.start();
    }


    public void start() {
        switch (status) {
            case JUST_CREATED:

                status = HBaseClientStatus.STARTING;
                try {
                    config = HBaseConfiguration.create();
//                    runLoginAndRenewalThread(config);
                    status = HBaseClientStatus.STARTED;
                } catch (Exception e) {
                    status = HBaseClientStatus.START_FAILED;
                    LOG.error("Start HBase client failed.", e);
                }
                break;

            case STARTED:
                throw new IllegalStateException("HBaseClient has already started, you only need to start it once");

            default:
                throw new IllegalStateException("HBaseClient state is illegal. State Status: " + status);
        }

    }


    public void insert(HBaseData... entries)
            throws HBasePersistenceException, HBaseDataInvalidException {
        insertBatch(Arrays.asList(entries));
    }

    public void insertBatch(Collection<HBaseData> collection)
            throws HBasePersistenceException, HBaseDataInvalidException {
        if (null == collection || collection.isEmpty()) {
            return;
        }

        checkHBaseClientState();

        HTable hTable = null;
        try {
            HBaseData sampleData = collection.iterator().next();

            if (!isHBaseDataRowValid(sampleData)) {
                throw new HBaseDataInvalidException("Data invalid");
            }

            hTable = new HTable(config, sampleData.getTable());
            List<Put> putList = new ArrayList<>(collection.size());
            for (HBaseData entry : collection) {
                if (!isHBaseDataRowValid(entry)) {
                    throw new HBaseDataInvalidException("Data invalid");
                }

                Put put = new Put(entry.getRowKey().getBytes());
                for (Map.Entry<String, byte[]> column : entry.getData().entrySet()) {
                    put.add(sampleData.getColumnFamily().getBytes(), column.getKey().getBytes(), column.getValue());
                }
                putList.add(put);
            }
            hTable.put(putList);
        } catch (IOException e) {
            LOG.error("Failed to create a HTable instance for operation", e);
            throw new HBasePersistenceException(e);
        } finally {
            if (null != hTable) {
                try {
                    hTable.close();
                } catch (IOException e) {
                    LOG.error("Failed while closing HTable.", e);
                }
            }
        }
    }

    private static boolean isHBaseDataRowValid(HBaseData data) {
        boolean invalid = null == data
                || null == data.getTable() || data.getTable().trim().isEmpty()
                || null == data.getColumnFamily() || data.getColumnFamily().trim().isEmpty()
                || null == data.getData() || data.getData().isEmpty();
        return !invalid;
    }

    private void checkHBaseClientState() {
        if (HBaseClientStatus.STARTED != status) {
            throw new IllegalStateException("Client State Illegal.");
        }
    }

    public List<HBaseData> query(String offerId, String affiliateId, Calendar start, Calendar end, String table,
                                 String columnFamily) throws HBasePersistenceException {
        checkHBaseClientState();
        HTable hTable = null;
        try {
            hTable = new HTable(config, table);
            Get get = new Get(Helper.generateKeyForHBase(offerId, affiliateId).getBytes());
            get.setTimeRange(start.getTimeInMillis(), end.getTimeInMillis());
            get.addFamily(columnFamily.getBytes());

            Result result = hTable.get(get);

            Cell[] cells = result.rawCells();
            List<HBaseData> hBaseDataList = null;
            if (null != cells) {
                hBaseDataList = new ArrayList<>(cells.length);
                for (Cell cell : cells) {
                    HBaseData hBaseData = new HBaseData();
                    hBaseData.setTable(table);
                    hBaseData.setColumnFamily(columnFamily);
                    hBaseData.setRowKey(Helper.generateKeyForHBase(offerId, affiliateId));
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTimeInMillis(cell.getTimestamp());
                    hBaseData.setDate(calendar.getTime());
                    Map<String, byte[]> data = new HashMap<>();
                    data.put(new String(CellUtil.cloneQualifier(cell)), CellUtil.cloneValue(cell));
                    hBaseData.setData(data);
                    hBaseData.setData(data);
                }
            }
            return hBaseDataList;
        } catch (IOException e) {
            LOG.error("HBase HTable instantiation failed.", e);
            throw new HBasePersistenceException(e);
        } finally {
            if (null != hTable) {
                try {
                    hTable.close();
                } catch (IOException e) {
                    LOG.error("Error close HTable", e);
                }
            }
        }
    }

    public List<HBaseData> scan(String offerId, Calendar start, Calendar end, String table, String columnFamily)
            throws HBasePersistenceException {
        checkHBaseClientState();
        HTable hTable = null;
        try {
            hTable = new HTable(config, table);
            Scan scan = new Scan();
            scan.addFamily(columnFamily.getBytes());
            scan.setTimeRange(start.getTimeInMillis(), end.getTimeInMillis());
            Filter filter = new ColumnPrefixFilter(offerId.getBytes());
            scan.setFilter(filter);
            ResultScanner results = hTable.getScanner(scan);
            List<HBaseData> result = new ArrayList<>();
            for (Result r : results) {
                HBaseData dataRow = new HBaseData();
                Map<String, byte[]> data = new HashMap<>();
                dataRow.setData(data);
                dataRow.setRowKey(new String(r.getRow(), DEFAULT_CHARSET));
                dataRow.setTable(table);
                dataRow.setColumnFamily(columnFamily);
                for (Cell cell : r.rawCells()) {
                    data.put(new String(CellUtil.cloneQualifier(cell)), CellUtil.cloneValue(cell));
                }
                result.add(dataRow);
            }
            return result;
        } catch (IOException e) {
            LOG.error("HBase HTable instantiation failed.", e);
            throw new HBasePersistenceException(e);
        } finally {
            if (null != hTable) {
                try {
                    hTable.close();
                } catch (IOException e) {
                    LOG.error("Error close HTable", e);
                }
            }
        }
    }

    /**
     *
     * @param offerId
     * @param affiliateId
     * @param start
     * @param end
     * @param table
     * @param columnFamily
     * @return
     */
    @Deprecated
    public List<HBaseData> scan(String offerId, String affiliateId, Calendar start, Calendar end,
                                String table, String columnFamily) throws HBasePersistenceException {
        checkHBaseClientState();
        HTable hTable = null;
        try {
            hTable = new HTable(config, table);
            Scan scan = new Scan();
            scan.addFamily(columnFamily.getBytes(DEFAULT_CHARSET));

            //because we use long max - timestamp like row key so the last one is the first one.
            byte[] stopRowKey = Helper.generateKeyForRedis(offerId, affiliateId, start.getTimeInMillis() + "").getBytes(DEFAULT_CHARSET);
            byte[] startRowKey = Helper.generateKeyForRedis(offerId, affiliateId, end.getTimeInMillis() + "").getBytes(DEFAULT_CHARSET);

            scan.setStartRow(startRowKey);
            scan.setStopRow(stopRowKey);
            scan.setMaxVersions(1);
            ResultScanner results = hTable.getScanner(scan);
            List<HBaseData> result = new ArrayList<>();
            for (Result r : results) {
                HBaseData dataRow = new HBaseData();
                Map<String, byte[]> data = new HashMap<>();
                dataRow.setData(data);
                dataRow.setRowKey(new String(r.getRow(), DEFAULT_CHARSET));
                dataRow.setTable(table);
                dataRow.setColumnFamily(columnFamily);
                for (Cell cell : r.rawCells()) {
                    data.put(new String(CellUtil.cloneQualifier(cell)), CellUtil.cloneValue(cell));
                }

                result.add(dataRow);
            }
            return result;
        } catch (IOException e) {
            LOG.error("HBase HTable instantiation failed.", e);
            throw new HBasePersistenceException(e);
        } finally {
            if (null != hTable) {
                try {
                    hTable.close();
                } catch (IOException e) {
                    LOG.error("Error close HTable", e);
                }
            }

        }
    }
}