package com.uso.hbase.template;

import com.uso.hbase.converter.ConversionService;
import com.uso.hbase.converter.DefaultConversionService;
import com.uso.hbase.exception.HBaseConnectionException;
import com.uso.hbase.exception.HBaseQueryException;
import com.uso.hbase.exception.HBaseWriteException;
import com.uso.hbase.metadata.ColumnInfo;
import com.uso.hbase.metadata.TableInfo;
import com.uso.hbase.page.Page;
import com.uso.hbase.page.Pageable;
import com.uso.hbase.query.Query;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * HBase 操作模板，类似 Spring 的 JdbcTemplate
 *
 * @author pengchuanjiang
 */
public class HBaseTemplate implements HBaseOperations {

    private static final Logger logger = LoggerFactory.getLogger(HBaseTemplate.class);

    private Connection connection;

    private ConversionService conversionService;

    public HBaseTemplate(Configuration configuration) {
        this(configuration, new DefaultConversionService());
    }

    public HBaseTemplate(Configuration configuration, ConversionService conversionService) {
        try {
            this.connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            throw new HBaseConnectionException("Failed to create HBase connection", e);
        }
        this.conversionService = conversionService;
    }

    public HBaseTemplate(Connection connection) {
        this(connection, new DefaultConversionService());
    }

    public HBaseTemplate(Connection connection, ConversionService conversionService) {
        this.connection = connection;
        this.conversionService = conversionService;
    }

    @Override
    public <T> T get(Class<T> entityClass, byte[] rowkey) {
        TableInfo tableInfo = TableInfo.of(entityClass);
        String tableName = tableInfo.getTableName();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(rowkey);
            Result result = table.get(get);
            if (result == null || result.isEmpty()) {
                return null;
            }
            return convertToEntity(result, entityClass, tableInfo);
        } catch (IOException e) {
            throw new HBaseQueryException("Failed to get data from HBase", e);
        }
    }

    @Override
    public <T> List<T> multiGet(Class<T> entityClass, List<byte[]> rowkeys) {
        TableInfo tableInfo = TableInfo.of(entityClass);
        String tableName = tableInfo.getTableName();
        List<T> results = new ArrayList<>();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            List<Get> gets = new ArrayList<>();
            for (byte[] rowkey : rowkeys) {
                gets.add(new Get(rowkey));
            }
            Result[] hbaseResults = table.get(gets);
            for (Result result : hbaseResults) {
                if (result != null && !result.isEmpty()) {
                    results.add(convertToEntity(result, entityClass, tableInfo));
                }
            }
        } catch (IOException e) {
            throw new HBaseQueryException("Failed to multi get data from HBase", e);
        }
        return results;
    }

    @Override
    public <T> void save(T entity) {
        TableInfo tableInfo = TableInfo.of(entity.getClass());
        String tableName = tableInfo.getTableName();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = convertToPut(entity, tableInfo);
            table.put(put);
        } catch (IOException e) {
            throw new HBaseWriteException("Failed to save data to HBase", e);
        }
    }

    @Override
    public <T> void saveBatch(List<T> entities) {
        if (entities == null || entities.isEmpty()) {
            return;
        }

        TableInfo tableInfo = TableInfo.of(entities.get(0).getClass());
        String tableName = tableInfo.getTableName();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            List<Put> puts = new ArrayList<>();
            for (T entity : entities) {
                puts.add(convertToPut(entity, tableInfo));
            }
            table.put(puts);
        } catch (IOException e) {
            throw new HBaseWriteException("Failed to batch save data to HBase", e);
        }
    }

    @Override
    public <T> boolean update(T entity) {
        save(entity);
        return true;
    }

    @Override
    public <T> void delete(Class<T> entityClass, byte[] rowkey) {
        TableInfo tableInfo = TableInfo.of(entityClass);
        String tableName = tableInfo.getTableName();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Delete delete = new Delete(rowkey);
            table.delete(delete);
        } catch (IOException e) {
            throw new HBaseWriteException("Failed to delete data from HBase", e);
        }
    }

    @Override
    public <T> void deleteBatch(Class<T> entityClass, List<byte[]> rowkeys) {
        if (rowkeys == null || rowkeys.isEmpty()) {
            return;
        }

        TableInfo tableInfo = TableInfo.of(entityClass);
        String tableName = tableInfo.getTableName();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            List<Delete> deletes = new ArrayList<>();
            for (byte[] rowkey : rowkeys) {
                deletes.add(new Delete(rowkey));
            }
            table.delete(deletes);
        } catch (IOException e) {
            throw new HBaseWriteException("Failed to batch delete data from HBase", e);
        }
    }

    @Override
    public <T> void deleteByEntity(T entity) {
        try {
            TableInfo tableInfo = TableInfo.of(entity.getClass());
            ColumnInfo rowkeyColumn = tableInfo.getRowkeyColumn();
            if (rowkeyColumn != null) {
                Object rowkey = rowkeyColumn.getGetMethod().invoke(entity);
                byte[] rowkeyBytes = conversionService.convert(rowkey);
                delete(entity.getClass(), rowkeyBytes);
            }
        } catch (Exception e) {
            throw new HBaseWriteException("Failed to delete entity", e);
        }
    }

    @Override
    public <T> void deleteAll(Class<T> entityClass) {
        List<T> allEntities = findAll(entityClass);
        if (allEntities.isEmpty()) {
            return;
        }

        TableInfo tableInfo = TableInfo.of(entityClass);
        String tableName = tableInfo.getTableName();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            List<Delete> deletes = new ArrayList<>();
            for (T entity : allEntities) {
                ColumnInfo rowkeyColumn = tableInfo.getRowkeyColumn();
                if (rowkeyColumn != null) {
                    Object rowkey = rowkeyColumn.getGetMethod().invoke(entity);
                    byte[] rowkeyBytes = conversionService.convert(rowkey);
                    deletes.add(new Delete(rowkeyBytes));
                }
            }
            if (!deletes.isEmpty()) {
                table.delete(deletes);
            }
        } catch (Exception e) {
            throw new HBaseWriteException("Failed to delete all data from HBase", e);
        }
    }

    @Override
    public <T> void deleteByQuery(Class<T> entityClass, Query query) {
        List<T> entities = find(entityClass, query);
        if (entities.isEmpty()) {
            return;
        }

        TableInfo tableInfo = TableInfo.of(entityClass);
        String tableName = tableInfo.getTableName();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            List<Delete> deletes = new ArrayList<>();
            for (T entity : entities) {
                ColumnInfo rowkeyColumn = tableInfo.getRowkeyColumn();
                if (rowkeyColumn != null) {
                    Object rowkey = rowkeyColumn.getGetMethod().invoke(entity);
                    byte[] rowkeyBytes = conversionService.convert(rowkey);
                    deletes.add(new Delete(rowkeyBytes));
                }
            }
            if (!deletes.isEmpty()) {
                table.delete(deletes);
            }
        } catch (Exception e) {
            throw new HBaseWriteException("Failed to delete data by query from HBase", e);
        }
    }

    @Override
    public <T> List<T> find(Class<T> entityClass, Query query) {
        TableInfo tableInfo = TableInfo.of(entityClass);
        String tableName = tableInfo.getTableName();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = buildScan(query);
            try (ResultScanner scanner = table.getScanner(scan)) {
                List<T> results = new ArrayList<>();
                int count = 0;
                int offset = query.getOffset();
                int limit = query.getLimit();

                for (Result result : scanner) {
                    if (count < offset) {
                        count++;
                        continue;
                    }
                    if (limit > 0 && results.size() >= limit) {
                        break;
                    }
                    if (result != null && !result.isEmpty()) {
                        results.add(convertToEntity(result, entityClass, tableInfo));
                    }
                }
                return results;
            }
        } catch (IOException e) {
            throw new HBaseQueryException("Failed to query data from HBase", e);
        }
    }

    @Override
    public <T> Page<T> find(Class<T> entityClass, Query query, Pageable pageable) {
        Query pageQuery = Query.create()
            .startRow(query.getStartRow())
            .stopRow(query.getStopRow())
            .limit(pageable.getOffset() + pageable.getPageSize());

        for (com.uso.hbase.query.Criteria criteria : query.getCriteriaList()) {
            pageQuery.addCriteria(criteria);
        }

        List<T> content = find(entityClass, pageQuery);
        int total = content.size();

        if (total > pageable.getOffset()) {
            content = content.subList(pageable.getOffset(), Math.min(total, pageable.getOffset() + pageable.getPageSize()));
        } else {
            content = new ArrayList<>();
        }

        return Page.of(content, pageable, total);
    }

    @Override
    public <T> T findOne(Class<T> entityClass, Query query) {
        query.limit(1);
        List<T> results = find(entityClass, query);
        return results.isEmpty() ? null : results.get(0);
    }

    @Override
    public <T> List<T> findAll(Class<T> entityClass) {
        return find(entityClass, Query.create());
    }

    @Override
    public <T> Page<T> findAll(Class<T> entityClass, Pageable pageable) {
        return find(entityClass, Query.create(), pageable);
    }

    @Override
    public <T> long count(Class<T> entityClass, Query query) {
        TableInfo tableInfo = TableInfo.of(entityClass);
        String tableName = tableInfo.getTableName();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = buildScan(query);
            scan.setCaching(1000);
            try (ResultScanner scanner = table.getScanner(scan)) {
                long count = 0;
                for (Result result : scanner) {
                    if (result != null && !result.isEmpty()) {
                        count++;
                    }
                }
                return count;
            }
        } catch (IOException e) {
            throw new HBaseQueryException("Failed to count data from HBase", e);
        }
    }

    @Override
    public <T> long countAll(Class<T> entityClass) {
        return count(entityClass, Query.create());
    }

    @Override
    public <T> boolean exists(Class<T> entityClass, byte[] rowkey) {
        TableInfo tableInfo = TableInfo.of(entityClass);
        String tableName = tableInfo.getTableName();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(rowkey);
            return table.exists(get);
        } catch (IOException e) {
            throw new HBaseQueryException("Failed to check existence in HBase", e);
        }
    }

    @Override
    public <T> boolean exists(Class<T> entityClass, Query query) {
        query.limit(1);
        return !find(entityClass, query).isEmpty();
    }

    @Override
    public <T> T execute(TableCallback<T> action) {
        try {
            return action.doInTable(connection);
        } catch (IOException e) {
            throw new HBaseQueryException("Failed to execute table action", e);
        }
    }

    @Override
    public <T> T executeScan(Class<?> entityClass, ScanCallback<T> callback) {
        TableInfo tableInfo = TableInfo.of(entityClass);
        String tableName = tableInfo.getTableName();

        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            try (ResultScanner scanner = table.getScanner(scan)) {
                return callback.doInScan(scan, scanner);
            }
        } catch (IOException e) {
            throw new HBaseQueryException("Failed to execute scan callback", e);
        }
    }

    @Override
    public boolean tableExists(String tableName) {
        try (Admin admin = connection.getAdmin()) {
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new HBaseQueryException("Failed to check table existence", e);
        }
    }

    @Override
    public void createTable(String tableName, String... columnFamilies) {
        try (Admin admin = connection.getAdmin()) {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String family : columnFamilies) {
                descriptor.addFamily(new HColumnDescriptor(family));
            }
            admin.createTable(descriptor);
        } catch (IOException e) {
            throw new HBaseWriteException("Failed to create table: " + tableName, e);
        }
    }

    @Override
    public void dropTable(String tableName) {
        try (Admin admin = connection.getAdmin()) {
            TableName tn = TableName.valueOf(tableName);
            if (admin.isTableEnabled(tn)) {
                admin.disableTable(tn);
            }
            admin.deleteTable(tn);
        } catch (IOException e) {
            throw new HBaseWriteException("Failed to drop table: " + tableName, e);
        }
    }

    @Override
    public void disableTable(String tableName) {
        try (Admin admin = connection.getAdmin()) {
            admin.disableTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new HBaseWriteException("Failed to disable table: " + tableName, e);
        }
    }

    @Override
    public void enableTable(String tableName) {
        try (Admin admin = connection.getAdmin()) {
            admin.enableTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new HBaseWriteException("Failed to enable table: " + tableName, e);
        }
    }

    /**
     * 获取连接
     */
    public Connection getConnection() {
        return connection;
    }

    private Scan buildScan(Query query) {
        Scan scan = new Scan();

        if (query.getStartRow() != null) {
            scan.withStartRow(query.getStartRow(), query.isIncludeStartRow());
        }
        if (query.getStopRow() != null) {
            scan.withStopRow(query.getStopRow(), query.isIncludeStopRow());
        }

        Filter filter = query.toFilter();
        if (filter != null) {
            scan.setFilter(filter);
        }

        scan.setMaxVersions(query.getMaxVersions());

        for (String family : query.getFamilies()) {
            scan.addFamily(Bytes.toBytes(family));
        }

        for (String column : query.getQualifiers()) {
            String[] parts = column.split(":");
            if (parts.length == 2) {
                scan.addColumn(Bytes.toBytes(parts[0]), Bytes.toBytes(parts[1]));
            }
        }

        return scan;
    }

    private <T> T convertToEntity(Result result, Class<T> entityClass, TableInfo tableInfo) {
        try {
            T entity = entityClass.newInstance();

            ColumnInfo rowkeyColumn = tableInfo.getRowkeyColumn();
            if (rowkeyColumn != null) {
                Object value = conversionService.from(result.getRow(), rowkeyColumn.getFieldType());
                rowkeyColumn.getSetMethod().invoke(entity, value);
            }

            for (ColumnInfo columnInfo : tableInfo.getDataColumns()) {
                byte[] value = result.getValue(columnInfo.getFamilyBytes(), columnInfo.getQualifierBytes());
                if (value != null) {
                    Object convertedValue = conversionService.from(value, columnInfo.getFieldType());
                    columnInfo.getSetMethod().invoke(entity, convertedValue);
                }
            }

            ColumnInfo timestampColumn = tableInfo.getTimestampColumn();
            if (timestampColumn != null && result.rawCells() != null && result.rawCells().length > 0) {
                long timestamp = result.rawCells()[0].getTimestamp();
                timestampColumn.getSetMethod().invoke(entity, timestamp);
            }

            return entity;
        } catch (Exception e) {
            throw new HBaseQueryException("Failed to convert Result to entity", e);
        }
    }

    private <T> Put convertToPut(T entity, TableInfo tableInfo) {
        try {
            ColumnInfo rowkeyColumn = tableInfo.getRowkeyColumn();
            if (rowkeyColumn == null) {
                throw new HBaseWriteException("Rowkey column not found");
            }

            Object rowkeyValue = rowkeyColumn.getGetMethod().invoke(entity);
            if (rowkeyValue == null) {
                throw new HBaseWriteException("Rowkey value cannot be null");
            }

            byte[] rowkey = conversionService.convert(rowkeyValue);
            Put put = new Put(rowkey);

            for (ColumnInfo columnInfo : tableInfo.getDataColumns()) {
                Object value = columnInfo.getGetMethod().invoke(entity);
                if (value != null) {
                    byte[] bytes = conversionService.convert(value);
                    put.addColumn(columnInfo.getFamilyBytes(), columnInfo.getQualifierBytes(), bytes);
                }
            }

            ColumnInfo timestampColumn = tableInfo.getTimestampColumn();
            if (timestampColumn != null) {
                Long timestamp = (Long) timestampColumn.getGetMethod().invoke(entity);
                if (timestamp != null) {
                    put.setTimestamp(timestamp);
                }
            }

            return put;
        } catch (Exception e) {
            throw new HBaseWriteException("Failed to convert entity to Put", e);
        }
    }

    public void setConversionService(ConversionService conversionService) {
        this.conversionService = conversionService;
    }

    public ConversionService getConversionService() {
        return conversionService;
    }

    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                logger.error("Failed to close HBase connection", e);
            }
        }
    }

}
