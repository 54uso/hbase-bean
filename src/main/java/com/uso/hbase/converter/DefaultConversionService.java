package com.uso.hbase.converter;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 默认类型转换服务，内置支持所有基本类型
 *
 * @author pengchuanjiang
 */
public class DefaultConversionService extends GenericConversionService {

    public DefaultConversionService() {
        addDefaultConverters(this);
    }

    public static void addDefaultConverters(ConverterRegistry converterRegistry) {
        converterRegistry.addConverter(new IntegerConvert());
        converterRegistry.addConverter(new StringConverter());
        converterRegistry.addConverter(new LongConvert());
        converterRegistry.addConverter(new FloatConvert());
        converterRegistry.addConverter(new DoubleConverter());
        converterRegistry.addConverter(new BooleanConvert());
        converterRegistry.addConverter(new ShortConvert());
        converterRegistry.addConverter(new BigDecimalConverter());
        converterRegistry.addConverter(new ByteBufferConvert());
        converterRegistry.addConverter(new ByteArrayConverter());
        converterRegistry.addConverter(new DateConverter());
        converterRegistry.addConverter(new ByteConverter());
        converterRegistry.addConverter(new CharacterConverter());
    }

    /**
     * 添加枚举类型转换器
     */
    public static <E extends Enum<E>> void addEnumConverter(ConverterRegistry converterRegistry, Class<E> enumType) {
        converterRegistry.addConverter(new EnumConvert<>(enumType));
    }

    private static final class IntegerConvert implements Converter<Integer> {

        @Override
        public byte[] convert(final Integer source) {
            return Bytes.toBytes(source);
        }

        @Override
        public Integer from(final byte[] bytes) {
            return Bytes.toInt(bytes);
        }

    }

    private static final class StringConverter implements Converter<String> {

        @Override
        public byte[] convert(final String source) {
            return Bytes.toBytes(source);
        }

        @Override
        public String from(final byte[] bytes) {
            return Bytes.toString(bytes);
        }

    }

    private static final class LongConvert implements Converter<Long> {

        @Override
        public byte[] convert(final Long source) {
            return Bytes.toBytes(source);
        }

        @Override
        public Long from(final byte[] bytes) {
            return Bytes.toLong(bytes);
        }

    }

    private static final class FloatConvert implements Converter<Float> {

        @Override
        public byte[] convert(final Float source) {
            return Bytes.toBytes(source);
        }

        @Override
        public Float from(final byte[] bytes) {
            return Bytes.toFloat(bytes);
        }

    }

    private static final class DoubleConverter implements Converter<Double> {

        @Override
        public byte[] convert(final Double source) {
            return Bytes.toBytes(source);
        }

        @Override
        public Double from(final byte[] bytes) {
            return Bytes.toDouble(bytes);
        }

    }

    private static final class BooleanConvert implements Converter<Boolean> {

        @Override
        public byte[] convert(final Boolean source) {
            return Bytes.toBytes(source);
        }

        @Override
        public Boolean from(final byte[] bytes) {
            return Bytes.toBoolean(bytes);
        }

    }

    private static final class ShortConvert implements Converter<Short> {

        @Override
        public byte[] convert(final Short source) {
            return Bytes.toBytes(source);
        }

        @Override
        public Short from(final byte[] bytes) {
            return Bytes.toShort(bytes);
        }

    }

    private static final class BigDecimalConverter implements Converter<BigDecimal> {

        @Override
        public byte[] convert(final BigDecimal source) {
            return Bytes.toBytes(source);
        }

        @Override
        public BigDecimal from(final byte[] bytes) {
            return Bytes.toBigDecimal(bytes);
        }

    }

    private static final class ByteBufferConvert implements Converter<ByteBuffer> {

        @Override
        public byte[] convert(final ByteBuffer source) {
            return Bytes.toBytes(source);
        }

        @Override
        public ByteBuffer from(final byte[] bytes) {
            return ByteBuffer.wrap(bytes);
        }

    }

    private static final class ByteArrayConverter implements Converter<byte[]> {

        @Override
        public byte[] convert(final byte[] source) {
            return source;
        }

        @Override
        public byte[] from(final byte[] bytes) {
            return bytes;
        }

    }

    private static final class DateConverter implements Converter<Date> {

        @Override
        public byte[] convert(final Date source) {
            return Bytes.toBytes(source.getTime());
        }

        @Override
        public Date from(final byte[] bytes) {
            return new Date(Bytes.toLong(bytes));
        }

    }

    private static final class ByteConverter implements Converter<Byte> {

        @Override
        public byte[] convert(final Byte source) {
            return new byte[]{source};
        }

        @Override
        public Byte from(final byte[] bytes) {
            return bytes[0];
        }

    }

    private static final class CharacterConverter implements Converter<Character> {

        @Override
        public byte[] convert(final Character source) {
            return new byte[]{(byte) source.charValue()};
        }

        @Override
        public Character from(final byte[] bytes) {
            return (char) bytes[0];
        }

    }

    /**
     * 泛型枚举转换器
     */
    private static final class EnumConvert<E extends Enum<E>> implements Converter<E> {

        private final Class<E> enumType;

        EnumConvert(Class<E> enumType) {
            this.enumType = enumType;
        }

        @Override
        public byte[] convert(final E source) {
            return Bytes.toBytes(source.name());
        }

        @Override
        public E from(final byte[] bytes) {
            String name = Bytes.toString(bytes);
            return Enum.valueOf(enumType, name);
        }

    }

}
