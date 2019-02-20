package com.uso.hbase.converter;

import com.uso.hbase.converter.exception.NotFoundConverterException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.Setter;

public class GenericConversionService implements ConversionService, ConverterRegistry {

    @Setter
    private List<Converter> converters = new LinkedList<>();

    private Map<Class, Converter> converterCache = new HashMap<>();

    @Override
    public <T> byte[] convert(final T source) {
        Converter converter = this.getConverter(source.getClass());
        return converter.convert(source);
    }

    @Override
    public <T> T from(final byte[] bytes, final Class<T> type) {
        Converter converter = getConverter(type);
        return (T) converter.from(bytes);
    }

    @Override
    public void addConverter(final Converter<?> converter) {
        this.converters.add(converter);
    }

    private Converter getConverter(final Class<?> clazz) {
        if (!this.converterCache.containsKey(clazz)) {
            this.converterCache.put(clazz, findConverter(clazz));
        }
        Converter converter = this.converterCache.get(clazz);
        if (converter == null) {
            throw new NotFoundConverterException(clazz);
        }
        return converter;
    }

    private Converter findConverter(final Class<?> clazz) {
        Class type = GenericConversionService.getPrimitiveWrapperMap().getOrDefault(clazz, clazz);
        for (Converter converter : this.converters) {
            Class<?> clz = getGenericClass(converter);
            if (type == clz) {
                return converter;
            }
        }
        return null;
    }

    private Class<?> getGenericClass(final Converter converter) {
        Type[] types = converter.getClass().getGenericInterfaces();
        if (types == null) {
            return null;
        }
        for (Type type : types) {
            Class<?> clazz = getConverterTypeArgument(type);
            if (clazz != null) {
                return clazz;
            }
        }
        return null;
    }

    private Class<?> getConverterTypeArgument(final Type type) {
        if (type instanceof ParameterizedType) {
            ParameterizedType pType = (ParameterizedType) type;
            if (!((Class) pType.getRawType()).isAssignableFrom(Converter.class)) {
                return null;
            }
            Type[] types = pType.getActualTypeArguments();
            if (types.length == 1) {
                return (Class) types[0];
            }
        }
        return null;
    }

    private static Map<Class, Class> getPrimitiveWrapperMap() {
        Map<Class, Class> typeMap = new HashMap<>();
        typeMap.put(int.class, Integer.class);
        typeMap.put(long.class, Long.class);
        typeMap.put(boolean.class, Boolean.class);
        typeMap.put(byte.class, Byte.class);
        typeMap.put(char.class, Character.class);
        typeMap.put(short.class, Short.class);
        typeMap.put(double.class, Double.class);
        typeMap.put(float.class, Float.class);
        return typeMap;
    }

}
