package com.uso.hbase.util;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase 工具类
 *
 * @author pengchuanjiang
 */
public final class HBaseUtils {

    private HBaseUtils() {
    }

    /**
     * 构建行键
     */
    public static byte[] buildRowKey(Object... parts) {
        if (parts == null || parts.length == 0) {
            return new byte[0];
        }

        int totalLength = 0;
        for (Object part : parts) {
            if (part != null) {
                totalLength += objectToBytes(part).length;
            }
        }

        byte[] rowKey = new byte[totalLength];
        int offset = 0;
        for (Object part : parts) {
            if (part != null) {
                byte[] bytes = objectToBytes(part);
                System.arraycopy(bytes, 0, rowKey, offset, bytes.length);
                offset += bytes.length;
            }
        }

        return rowKey;
    }

    /**
     * 构建带分隔符的行键
     */
    public static String buildRowKeyWithSeparator(String separator, Object... parts) {
        if (parts == null || parts.length == 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (parts[i] != null) {
                sb.append(parts[i].toString());
                if (i < parts.length - 1) {
                    sb.append(separator);
                }
            }
        }

        return sb.toString();
    }

    /**
     * 将对象转换为字节数组
     */
    public static byte[] objectToBytes(Object obj) {
        if (obj == null) {
            return new byte[0];
        }
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        }
        if (obj instanceof String) {
            return Bytes.toBytes((String) obj);
        }
        if (obj instanceof Integer) {
            return Bytes.toBytes((Integer) obj);
        }
        if (obj instanceof Long) {
            return Bytes.toBytes((Long) obj);
        }
        if (obj instanceof Double) {
            return Bytes.toBytes((Double) obj);
        }
        if (obj instanceof Float) {
            return Bytes.toBytes((Float) obj);
        }
        if (obj instanceof Boolean) {
            return Bytes.toBytes((Boolean) obj);
        }
        if (obj instanceof Short) {
            return Bytes.toBytes((Short) obj);
        }
        return Bytes.toBytes(obj.toString());
    }

    /**
     * 将字节数组转换为指定类型
     */
    @SuppressWarnings("unchecked")
    public static <T> T bytesToObject(byte[] bytes, Class<T> type) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        if (type == byte[].class) {
            return (T) bytes;
        }
        if (type == String.class) {
            return (T) Bytes.toString(bytes);
        }
        if (type == Integer.class || type == int.class) {
            return (T) Integer.valueOf(Bytes.toInt(bytes));
        }
        if (type == Long.class || type == long.class) {
            return (T) Long.valueOf(Bytes.toLong(bytes));
        }
        if (type == Double.class || type == double.class) {
            return (T) Double.valueOf(Bytes.toDouble(bytes));
        }
        if (type == Float.class || type == float.class) {
            return (T) Float.valueOf(Bytes.toFloat(bytes));
        }
        if (type == Boolean.class || type == boolean.class) {
            return (T) Boolean.valueOf(Bytes.toBoolean(bytes));
        }
        if (type == Short.class || type == short.class) {
            return (T) Short.valueOf(Bytes.toShort(bytes));
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    /**
     * 检查字节数组是否为空
     */
    public static boolean isEmpty(byte[] bytes) {
        return bytes == null || bytes.length == 0;
    }

    /**
     * 检查字符串是否为空
     */
    public static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    /**
     * 连接字节数组
     */
    public static byte[] concat(byte[]... arrays) {
        int totalLength = 0;
        for (byte[] array : arrays) {
            if (array != null) {
                totalLength += array.length;
            }
        }

        byte[] result = new byte[totalLength];
        int offset = 0;
        for (byte[] array : arrays) {
            if (array != null) {
                System.arraycopy(array, 0, result, offset, array.length);
                offset += array.length;
            }
        }

        return result;
    }

    /**
     * 获取子数组
     */
    public static byte[] subArray(byte[] array, int start, int length) {
        if (array == null || start < 0 || length < 0 || start + length > array.length) {
            throw new IllegalArgumentException("Invalid array or parameters");
        }
        byte[] result = new byte[length];
        System.arraycopy(array, start, result, 0, length);
        return result;
    }

    /**
     * 比较两个字节数组
     */
    public static int compare(byte[] a, byte[] b) {
        return Bytes.compareTo(a, b);
    }

    /**
     * 字节数组转十六进制字符串
     */
    public static String toHexString(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * 十六进制字符串转字节数组
     */
    public static byte[] fromHexString(String hex) {
        if (hex == null || hex.isEmpty()) {
            return new byte[0];
        }
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

}
