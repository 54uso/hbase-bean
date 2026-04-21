package com.uso.hbase.converter;

import com.uso.hbase.converter.exception.NotFoundConverterException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.Setter;

/**
 * 通用类型转换服务
 *
 * @author pengchuanjiang
 */
public class GenericConversionService implements ConversionService, ConverterRegistry {

    @Setter
    private List<Converter> converters = new LinkedList<>();

    private Map<Class, Converter> converterCache = new HashMap<>();

    private static final Map<Class, Class> PRIMITIVE_WRAPPER_MAP = new HashMap<>();

    static {
        PRIMITIVE_WRAPPER_MAP.put(int.class, Integer.class);
        PRIMITIVE_WRAPPER_MAP.put(long.class, Long.class);
        PRIMITIVE_WRAPPER_MAP.put(boolean.class, Boolean.class);
        PRIMITIVE_WRAPPER_MAP.put(byte.class, Byte.class);
        PRIMITIVE_WRAPPER_MAP.put(char.class, Character.class);
        PRIMITIVE_WRAPPER_MAP.put(short.class, Short.class);
        PRIMITIVE_WRAPPER_MAP.put(double.class, Double.class);
        PRIMITIVE_WRAPPER_MAP.put(float.class, Float.class);
    }

    @Override
    public <T> byte[] convert(final T source) {
        if (source == null) {
            return null;
        }
        Converter converter = this.getConverter(source.getClass());
        return converter.convert(source);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T from(final byte[] bytes, final Class<T> type) {
        if (bytes == null) {
            return null;
        }
        // 处理枚举类型
        if (type.isEnum()) {
            return (T) Enum.valueOf((Class<Enum>) type, new String(bytes));
        }
        Converter converter = getConverter(type);
        if (converter == null) {
            throw new NotFoundConverterException(type);
        }
        return (T) converter.from(bytes);
    }

    @Override
    public void addConverter(final Converter<?> converter) {
        this.converters.add(converter);
        // 清除缓存以便重新查找
        this.converterCache.clear();
    }

    /**
     * 移除转换器
     */
    public void removeConverter(final Converter<?> converter) {
        this.converters.remove(converter);
        this.converterCache.clear();
    }

    /**
     * 清除所有转换器
     */
    public void clearConverters() {
        this.converters.clear();
        this.converterCache.clear();
    }

    /**
     * 是否可以转换指定类型
     */
    public boolean canConvert(final Class<?> sourceType, final Class<?> targetType) {
        if (sourceType == null || targetType == null) {
            return false;
        }
        // byte[] 目标类型
        if (byte[].class == targetType) {
            return getConverter(sourceType) != null;
        }
        // byte[] 源类型
        if (byte[].class == sourceType) {
            return getConverter(targetType) != null;
        }
        return false;
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
        Class<?> type = getPrimitiveWrapperMap().getOrDefault(clazz, clazz);
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
        return PRIMITIVE_WRAPPER_MAP;
    }

}
