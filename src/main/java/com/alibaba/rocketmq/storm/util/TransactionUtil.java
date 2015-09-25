package com.alibaba.rocketmq.storm.util;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

/**
 * Created by holly on 3/17/15.
 */
public class TransactionUtil {

    private static final Logger logger = LoggerFactory.getLogger(TransactionUtil.class);

    public static final char BIT_ZERO = '0';

    public static final int LONG_SIZE = 64;

    public static final int CRC_SIZE = 8;

    public static final long CRYPT_SIZE = 16;

    private static int SECRET_SELECTION_SEED = 64;

    private static final long CRC_MAST = 0xFFl; // 8 bit for  11111111

    private static final long CRYPT_MAST = 0xFFFFl; // 16 bit

    /**
     * verify the transaction id
     *
     * @return verify result
     */
    public static boolean verify(String transactionId) {

        if ( decode(transactionId) == null ) {
            return false;
        }
        return true;
    }

    /**
     * decode transactionId to platformId,affiliateId,offerId
     *
     * @return array of [sequenceId,platformId,affiliateId,offerId]
     */
    public static long[] decode(String transactionId) {

        if ( StringUtils.isEmpty(transactionId) ) {
            return null;
        }
        transactionId = transactionId.replace("-", "");
        if ( transactionId.length() != 64 ) {
            logger.warn("transaction id size is not 64 ,{}", transactionId);
            return null;
        }

        try {

            String highValue = transactionId.substring(0, 16);
            String high0Value = transactionId.substring(16, 32);
            String low0Value = transactionId.substring(32, 48);
            String lowValue = transactionId.substring(48);

            BigInteger high = new BigInteger(highValue, 16);
            BigInteger high0 = new BigInteger(high0Value, 16);
            BigInteger low0 = new BigInteger(low0Value, 16);
            BigInteger low = new BigInteger(lowValue, 16);
            long[] v = decryptWrap(new long[] { high.longValue(), high0.longValue(), low0.longValue(), low.longValue() });
            if ( v == null ) {
                return null;
            }

            long crc = getCrc(v);
            long oldCrc = v[3] << (LONG_SIZE - CRC_SIZE - CRYPT_SIZE) >>> (LONG_SIZE - CRC_SIZE);
            if ( crc != oldCrc ) {
                logger.warn("transaction id crc check fail ,{}", transactionId);
                return null;
            }

            int count = 0;
            int year = (int) (v[0] << count >>> (LONG_SIZE - 7));
            count += 7;

            int month = (int) (v[0] << count >>> (LONG_SIZE - 4));
            count += 4;

            int day = (int) (v[0] << count >>> (LONG_SIZE - 5));
            count += 5;

            int hour = (int) (v[0] << count >>> (LONG_SIZE - 5));
            count += 5;

            int minute = (int) (v[0] << count >>> (LONG_SIZE - 6));
            count += 6;

            int second = (int) (v[0] << count >>> (LONG_SIZE - 6));
            count += 6; //now 33 bit
            DateTime t = new DateTime(year + 2015, month, day, hour, minute, second, DateTimeZone.UTC);

            long sequenceId = (long) (v[0] << 34 >>> 34);
            long platformId = (long) (v[1] >>> 32);
            long affiliateId = (long) (v[1] << 32 >>> 32);
            long offerId = (long) (v[2] >>> 32);
            return new long[] { t.getMillis(), sequenceId, platformId, affiliateId, offerId };

        } catch ( Exception e ) {
            logger.error("transaction id verify fail,{}", transactionId, e);
            return null;
        }
    }

    /**
     * generate crc bits
     *
     * @param longs high long and low long
     * @return crc bits, 8 bits
     */
    public static long getCrc(long[] longs) {

        long crc = EncryptKeys.crcKey;
        int crcCount = 1;

        for ( long data : new long[] { longs[0], longs[1], longs[2] } ) {
            while ( crcCount < LONG_SIZE / CRC_SIZE ) {
                long tmp = CRC_MAST << (LONG_SIZE - crcCount * CRC_SIZE);
                tmp &= data;
                tmp = tmp >>> (LONG_SIZE - crcCount * CRC_SIZE);
                crc ^= tmp;
                crcCount++;
            }
            crcCount = 1;
        }

        crcCount = 1;
        while ( crcCount < (LONG_SIZE - CRC_SIZE - CRYPT_SIZE) / CRC_SIZE ) {
            long tmp = CRC_MAST << (LONG_SIZE - crcCount * CRC_SIZE);
            tmp &= longs[3];
            tmp = tmp >>> (LONG_SIZE - crcCount * CRC_SIZE);
            crc ^= tmp;
            crcCount++;
        }
        return crc;
    }

    private static long[] decryptWrap(long[] values) {

        long high = values[0];
        long high0 = values[1];
        long low0 = values[2];
        long low = values[3];
        int index = (int) (CRYPT_MAST & low);
        if ( index > SECRET_SELECTION_SEED ) {
            return null;
        }
        byte[] keys = EncryptKeys.wrapKey[index];
        long highKeys0 = ((long) keys[0] << 56) + ((long) keys[1] << 48) + ((long) keys[2] << 40) +
                ((long) keys[3] << 32) + ((long) keys[4] << 24) + ((long) keys[5] << 16) +
                ((long) keys[6] << 8) + ((long) keys[7]);//64 bit

        long highKeys1 = ((long) keys[8] << 56) + ((long) keys[9] << 48) + ((long) keys[10] << 40) +
                ((long) keys[11] << 32) + ((long) keys[12] << 24) + ((long) keys[13] << 16) +
                ((long) keys[14] << 8) + ((long) keys[15]);//64 bit

        long highKeys2 = ((long) keys[16] << 56) + ((long) keys[17] << 48) + ((long) keys[18] << 40) +
                ((long) keys[19] << 32) + ((long) keys[20] << 24) + ((long) keys[21] << 16) +
                ((long) keys[22] << 8) + ((long) keys[23]);//64 bit

        long highKeys3 = ((long) keys[24] << 56) + ((long) keys[25] << 48) + ((long) keys[26] << 40) +
                ((long) keys[27] << 32) + ((long) keys[28] << 24) + ((long) keys[29] << 16) +
                0x0000l; //

        high ^= highKeys0;
        high0 ^= highKeys1;
        low0 ^= highKeys2;
        low ^= highKeys3;
        low &= ~CRYPT_MAST; //reset crypt bits to 0
        return new long[] { high, high0, low0, low };
    }

}
