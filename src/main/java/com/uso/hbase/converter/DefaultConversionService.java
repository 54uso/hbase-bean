package com.uso.hbase.converter;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.util.Bytes;

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

}
