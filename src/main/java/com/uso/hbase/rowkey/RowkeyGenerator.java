package com.uso.hbase.rowkey;

import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 行键生成器
 *
 * @author pengchuanjiang
 */
public class RowkeyGenerator {

    private static final AtomicLong COUNTER = new AtomicLong(0);

    private RowkeyGenerator() {
    }

    /**
     * 生成 UUID 行键
     */
    public static String generateUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 生成带前缀的 UUID 行键
     */
    public static String generateUUID(String prefix) {
        return prefix + "_" + generateUUID();
    }

    /**
     * 生成时间戳行键（反转时间戳，最新的数据在最前面）
     */
    public static String generateTimestampRowkey() {
        long timestamp = System.currentTimeMillis();
        long reverseTimestamp = Long.MAX_VALUE - timestamp;
        return String.format("%020d", reverseTimestamp);
    }

    /**
     * 生成带前缀的时间戳行键
     */
    public static String generateTimestampRowkey(String prefix) {
        return prefix + "_" + generateTimestampRowkey();
    }

    /**
     * 生成递增 ID 行键
     */
    public static String generateIncrementId() {
        long id = COUNTER.incrementAndGet();
        return String.format("%020d", id);
    }

    /**
     * 生成带前缀的递增 ID 行键
     */
    public static String generateIncrementId(String prefix) {
        return prefix + "_" + generateIncrementId();
    }

    /**
     * 生成组合行键（前缀 + 时间戳 + 随机数）
     */
    public static String generateCompositeRowkey(String prefix, String separator) {
        StringBuilder sb = new StringBuilder();
        if (prefix != null && !prefix.isEmpty()) {
            sb.append(prefix).append(separator);
        }
        long timestamp = System.currentTimeMillis();
        long reverseTimestamp = Long.MAX_VALUE - timestamp;
        sb.append(String.format("%020d", reverseTimestamp));
        sb.append(separator);
        sb.append(String.format("%010d", (int) (Math.random() * 1000000000)));
        return sb.toString();
    }

    /**
     * 生成带日期的行键
     */
    public static String generateDateRowkey(String prefix) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String dateStr = sdf.format(new Date());
        return prefix + "_" + dateStr + "_" + (int) (Math.random() * 10000);
    }

    /**
     * 生成带散列的行键（用于避免热点）
     */
    public static String generateHashedRowkey(String originalKey) {
        int hashCode = Math.abs(originalKey.hashCode());
        int bucket = hashCode % 16;
        return String.format("%01x_%s", bucket, originalKey);
    }

    /**
     * 生成带盐值的行键（用于避免热点）
     */
    public static String generateSaltedRowkey(String originalKey, int bucketCount) {
        int hashCode = Math.abs(originalKey.hashCode());
        int bucket = hashCode % bucketCount;
        return String.format("%04d_%s", bucket, originalKey);
    }

    /**
     * 反转字符串行键
     */
    public static String reverseRowkey(String rowkey) {
        if (rowkey == null) {
            return null;
        }
        return new StringBuilder(rowkey).reverse().toString();
    }

    /**
     * 将行键转换为字节数组
     */
    public static byte[] toBytes(String rowkey) {
        return Bytes.toBytes(rowkey);
    }

    /**
     * 将行键转换为字节数组（使用指定分隔符拆分后取指定部分）
     */
    public static byte[] toBytes(String rowkey, String separator, int index) {
        String[] parts = rowkey.split(separator);
        if (index < 0 || index >= parts.length) {
            throw new IllegalArgumentException("Index out of bounds: " + index);
        }
        return Bytes.toBytes(parts[index]);
    }

}
