package com.uso.hbase.handler;

import com.uso.hbase.converter.ConversionService;
import com.uso.hbase.converter.DefaultConversionService;
import com.uso.hbase.handler.exception.HProcessException;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public class DefaultHandlerFactory implements HandlerFactory {

    @Setter
    @Getter
    private ConversionService conversionService = new DefaultConversionService();

    private Map<Class, HRowHandler> handlerCaches;

    public DefaultHandlerFactory() {
        handlerCaches = new HashMap<>();
    }

    @Override
    public <T> T convert(final Result result, Class<T> clazz) throws HProcessException {
        try {
            return getHRowHandler(clazz).convert(result);
        } catch (final Exception ex) {
            throw new HProcessException(ex);
        }
    }

    @Override
    public Put buildPut(final Object source) throws HProcessException {
        try {
            HRowHandler handler = getHRowHandler(source.getClass());
            return handler.buildPut(source);
        } catch (final Exception ex) {
            throw new HProcessException(ex);
        }
    }

    private <T> HRowHandler<T> getHRowHandler(final Class<T> clazz) throws InstantiationException {
        if (!handlerCaches.containsKey(clazz)) {
            synchronized (this) {
                if (!handlerCaches.containsKey(clazz)) {
                    handlerCaches.put(clazz, new HRowHandler<>(clazz, conversionService));
                }
            }
        }
        return handlerCaches.get(clazz);
    }

}
