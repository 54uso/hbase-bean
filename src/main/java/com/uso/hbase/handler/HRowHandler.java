package com.uso.hbase.handler;

import com.uso.hbase.annotation.HColumn;
import com.uso.hbase.annotation.HIgnore;
import com.uso.hbase.annotation.HRow;
import com.uso.hbase.annotation.HRowkey;
import com.uso.hbase.converter.ConversionService;
import com.uso.hbase.handler.exception.HbaseAnnotationException;
import com.uso.hbase.handler.exception.NotFoundRowkeyException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

@Slf4j
public class HRowHandler<T> {

    private Class<T> type;

    private Mapper<T> rowkeyMapper;

    private List<Mapper<T>> columnMappers;

    private ConversionService conversionService;

    public HRowHandler(final Class<T> type, final ConversionService conversionService) throws InstantiationException {
        try {
            if (type.getAnnotation(HRow.class) == null) {
                throw new Exception();
            }
            this.conversionService = conversionService;
            this.type = type;
            this.buildMappers();
        } catch (final Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new InstantiationException(HRowHandler.class.getName() + " init failed.");
        }
    }

    public Put buildPut(final T source) throws Exception {
        if (this.rowkeyMapper == null) {
            throw new NotFoundRowkeyException(this.type);
        }
        Put put = new Put(this.rowkeyMapper.getRowkey(source));
        for (Mapper<T> mapper : this.columnMappers) {
            mapper.addColumnToPut(put, source);
        }
        return put;
    }

    public T convert(final Result result) throws Exception {
        T obj = this.type.newInstance();
        this.rowkeyMapper.evalToTarget(obj, result);
        for (Mapper<T> mapper : this.columnMappers) {
            mapper.evalToTarget(obj, result);
        }
        return obj;
    }

    private void buildMappers() throws InstantiationException {
        List<Mapper<T>> cms = new LinkedList<>();
        Mapper<T> rm = null;
        Field[] fields = this.type.getDeclaredFields();
        boolean allField = this.type.getAnnotation(HRow.class).allField();
        for (Field field : fields) {
            if (!this.canMapper(field, allField)) {
                continue;
            }
            Mapper<T> mapper = new Mapper<>(this.type, field, this.conversionService);
            if (mapper.isRowkey()) {
                rm = mapper;
            }
            if (mapper.isColumn()) {
                cms.add(mapper);
            }
        }
        this.rowkeyMapper = rm;
        this.columnMappers = cms;
    }

    private boolean canMapper(final Field field, final boolean isAllField) {
        if (field.getAnnotation(HIgnore.class) != null) {
            return false;
        }
        if (field.getAnnotation(HRowkey.class) != null) {
            return true;
        }
        if (field.getAnnotation(HColumn.class) != null) {
            return true;
        }
        return isAllField;
    }

    private static final class Mapper<T> {

        private static final String FAMILY_EMPTY_FORMAT = "Annotation '%s' on %s.%s is not specified property 'family'";

        private ConversionService conversionService;

        private boolean isRowkey;

        private boolean isColumn;

        private Class<?> type;

        private Method getMethod;

        private Method setMethod;

        private byte[] family;

        private byte[] qualifier;

        private Mapper(final Class<T> clazz, final Field field, final ConversionService conversionService) throws InstantiationException {
            try {
                this.conversionService = conversionService;
                this.type = field.getType();
                this.getMethod = this.buildGetMethod(clazz, field);
                this.setMethod = this.buildSetMethod(clazz, field);
                this.isRowkey = field.getAnnotation(HRowkey.class) != null;
                this.isColumn = !this.isRowkey || field.getAnnotation(HColumn.class) != null;
                if (isColumn) {
                    this.family = this.getFamily(clazz, field);
                    this.qualifier = this.getQualifier(field);
                }
            } catch (final Exception ex) {
                log.error(ex.getMessage(), ex);
                throw new InstantiationException(Mapper.class.getName() + " init failed.");
            }
        }

        private void addColumnToPut(final Put put, final T source) throws ReflectiveOperationException {
            Object value = this.getMethod.invoke(source);
            if (value != null && this.isColumn) {
                put.addColumn(this.family, this.qualifier, this.convert(value));
            }
        }

        private void evalToTarget(final T target, final Result result) throws ReflectiveOperationException {
            if (result != null) {
                byte[] value;
                if (this.isRowkey) {
                    value = result.getRow();
                } else {
                    value = result.getValue(this.family, this.qualifier);
                }
                if (value != null) {
                    this.setMethod.invoke(target, from(value));
                }
            }
        }

        private byte[] getRowkey(final T source) throws ReflectiveOperationException {
            if (this.isRowkey) {
                return convert(this.getMethod.invoke(source));
            }
            return null;
        }

        private boolean isRowkey() {
            return this.isRowkey;
        }

        private boolean isColumn() {
            return this.isColumn;
        }

        private byte[] convert(final Object value) {
            if (value == null) {
                return null;
            }
            return this.conversionService.convert(value);
        }

        private Object from(final byte[] value) {
            if (value == null) {
                return null;
            }
            return this.conversionService.from(value, this.type);
        }

        private byte[] getFamily(final Class<T> clazz, final Field field) throws HbaseAnnotationException {
            String family = null;
            HRow hRow = clazz.getAnnotation(HRow.class);
            if (hRow != null && !isBlank(hRow.defaultFamily())) {
                family = hRow.defaultFamily();
            }
            HColumn hColumn = field.getAnnotation(HColumn.class);
            if (hColumn != null && !isBlank(hColumn.family())) {
                family = hColumn.family();
            }
            if (isBlank(family)) {
                throw new HbaseAnnotationException(String.format(
                    FAMILY_EMPTY_FORMAT, HColumn.class.getSimpleName(), clazz.getName(), field.getName()
                ));
            }
            return Bytes.toBytes(family);
        }

        private byte[] getQualifier(final Field field) {
            String qualifier = field.getName();
            HColumn hColumn = field.getAnnotation(HColumn.class);
            if (hColumn != null && !isBlank(hColumn.qualifier())) {
                qualifier = hColumn.qualifier();
            }
            return Bytes.toBytes(qualifier);
        }

        private Method buildGetMethod(final Class<T> clazz, final Field field) throws NoSuchMethodException {
            String name = field.getName();
            if (isBooleanType(field.getType())) {
                name = name.replaceAll("^is", "");
            }
            name = name.substring(0, 1).toUpperCase() + name.substring(1);
            String methodName = "get" + name;
            if (isBooleanType(field.getType())) {
                methodName = "is" + name;
            }
            return clazz.getDeclaredMethod(methodName);
        }

        private Method buildSetMethod(final Class<T> clazz, final Field field) throws NoSuchMethodException {
            String name = field.getName();
            if (Boolean.class == field.getType() || boolean.class == field.getType()) {
                name = name.replaceAll("^is", "");
            }
            String methodName = "set" + name.substring(0, 1).toUpperCase() + name.substring(1);
            return clazz.getDeclaredMethod(methodName, field.getType());
        }

        private static boolean isBlank(final String str) {
            return str == null || str.length() == 0;
        }

        private static boolean isBooleanType(final Class<?> type) {
            return Boolean.class == type || boolean.class == type;
        }

    }

}
