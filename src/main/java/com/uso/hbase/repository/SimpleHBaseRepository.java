package com.uso.hbase.repository;

import com.uso.hbase.metadata.ColumnInfo;
import com.uso.hbase.metadata.TableInfo;
import com.uso.hbase.page.Page;
import com.uso.hbase.page.Pageable;
import com.uso.hbase.query.Query;
import com.uso.hbase.template.HBaseTemplate;

import java.util.ArrayList;
import java.util.List;

/**
 * HBase Repository 基础实现类
 *
 * @author pengchuanjiang
 */
public class SimpleHBaseRepository<T, ID> implements HBaseRepository<T, ID> {

    private final HBaseTemplate hbaseTemplate;

    private final Class<T> entityClass;

    private final TableInfo tableInfo;

    public SimpleHBaseRepository(HBaseTemplate hbaseTemplate, Class<T> entityClass) {
        this.hbaseTemplate = hbaseTemplate;
        this.entityClass = entityClass;
        this.tableInfo = TableInfo.of(entityClass);
    }

    @Override
    public T findById(ID id) {
        byte[] rowkey = convertIdToBytes(id);
        return hbaseTemplate.get(entityClass, rowkey);
    }

    @Override
    public List<T> findAllById(Iterable<ID> ids) {
        List<byte[]> rowkeys = new ArrayList<>();
        for (ID id : ids) {
            rowkeys.add(convertIdToBytes(id));
        }
        return hbaseTemplate.multiGet(entityClass, rowkeys);
    }

    @Override
    public <S extends T> S save(S entity) {
        hbaseTemplate.save(entity);
        return entity;
    }

    @Override
    public <S extends T> Iterable<S> saveAll(Iterable<S> entities) {
        List<S> result = new ArrayList<>();
        List<T> batch = new ArrayList<>();
        for (S entity : entities) {
            batch.add(entity);
            result.add(entity);
            if (batch.size() >= 1000) {
                hbaseTemplate.saveBatch(batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            hbaseTemplate.saveBatch(batch);
        }
        return result;
    }

    @Override
    public boolean existsById(ID id) {
        byte[] rowkey = convertIdToBytes(id);
        return hbaseTemplate.exists(entityClass, rowkey);
    }

    @Override
    public List<T> findAll() {
        return hbaseTemplate.findAll(entityClass);
    }

    @Override
    public Page<T> findAll(Pageable pageable) {
        return hbaseTemplate.findAll(entityClass, pageable);
    }

    @Override
    public long count() {
        return hbaseTemplate.countAll(entityClass);
    }

    @Override
    public void deleteById(ID id) {
        byte[] rowkey = convertIdToBytes(id);
        hbaseTemplate.delete(entityClass, rowkey);
    }

    @Override
    public void delete(T entity) {
        hbaseTemplate.deleteByEntity(entity);
    }

    @Override
    public void deleteAll(Iterable<? extends T> entities) {
        List<byte[]> rowkeys = new ArrayList<>();
        try {
            ColumnInfo rowkeyColumn = tableInfo.getRowkeyColumn();
            if (rowkeyColumn != null) {
                for (T entity : entities) {
                    Object id = rowkeyColumn.getGetMethod().invoke(entity);
                    rowkeys.add(convertObjectToBytes(id));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to collect rowkeys for batch delete", e);
        }
        if (!rowkeys.isEmpty()) {
            hbaseTemplate.deleteBatch(entityClass, rowkeys);
        }
    }

    @Override
    public List<T> find(Query query) {
        return hbaseTemplate.find(entityClass, query);
    }

    @Override
    public Page<T> find(Query query, Pageable pageable) {
        return hbaseTemplate.find(entityClass, query, pageable);
    }

    @Override
    public T findOne(Query query) {
        return hbaseTemplate.findOne(entityClass, query);
    }

    @Override
    public long count(Query query) {
        return hbaseTemplate.count(entityClass, query);
    }

    @SuppressWarnings("unchecked")
    private byte[] convertIdToBytes(ID id) {
        if (id instanceof byte[]) {
            return (byte[]) id;
        }
        if (id instanceof String) {
            return ((String) id).getBytes();
        }
        if (id instanceof Long) {
            byte[] bytes = new byte[Long.BYTES];
            long value = (Long) id;
            for (int i = Long.BYTES - 1; i >= 0; i--) {
                bytes[i] = (byte) (value & 0xFF);
                value >>= 8;
            }
            return bytes;
        }
        if (id instanceof Integer) {
            byte[] bytes = new byte[Integer.BYTES];
            int value = (Integer) id;
            for (int i = Integer.BYTES - 1; i >= 0; i--) {
                bytes[i] = (byte) (value & 0xFF);
                value >>= 8;
            }
            return bytes;
        }
        return id.toString().getBytes();
    }

    @SuppressWarnings("unchecked")
    private byte[] convertObjectToBytes(Object obj) {
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        }
        if (obj instanceof String) {
            return ((String) obj).getBytes();
        }
        if (obj instanceof Long) {
            byte[] bytes = new byte[Long.BYTES];
            long value = (Long) obj;
            for (int i = Long.BYTES - 1; i >= 0; i--) {
                bytes[i] = (byte) (value & 0xFF);
                value >>= 8;
            }
            return bytes;
        }
        if (obj instanceof Integer) {
            byte[] bytes = new byte[Integer.BYTES];
            int value = (Integer) obj;
            for (int i = Integer.BYTES - 1; i >= 0; i--) {
                bytes[i] = (byte) (value & 0xFF);
                value >>= 8;
            }
            return bytes;
        }
        return obj.toString().getBytes();
    }

    protected HBaseTemplate getHbaseTemplate() {
        return hbaseTemplate;
    }

    protected Class<T> getEntityClass() {
        return entityClass;
    }

    protected TableInfo getTableInfo() {
        return tableInfo;
    }

}
