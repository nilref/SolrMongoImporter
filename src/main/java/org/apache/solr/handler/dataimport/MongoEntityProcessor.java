/**
 * Solr MongoDB data import handler
 */
package org.apache.solr.handler.dataimport;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;

/**
 * MongoDB entity processor (适配 MongoDB 4.x 驱动)
 */
public class MongoEntityProcessor extends EntityProcessorBase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoEntityProcessor.class); // 修正日志类引用

    /**
     * MongoDB datasource (适配 4.x 驱动的 MongoDataSource)
     */
    protected MongoDataSource dataSource;

    private String collection;

    /**
     * 初始化方法：获取集合名称并校验数据源
     */
    @Override
    public void init(Context context) {
        super.init(context);
        this.collection = context.getEntityAttribute(COLLECTION);
        if (this.collection == null || this.collection.trim().isEmpty()) {
            throw new DataImportHandlerException(SEVERE, "Collection name must be specified for MongoDB entity");
        }
        // 获取适配 4.x 驱动的数据源
        this.dataSource = (MongoDataSource) context.getDataSource();
        if (this.dataSource == null) {
            throw new DataImportHandlerException(SEVERE, "MongoDataSource is not configured");
        }
    }

    /**
     * 初始化查询：处理日期格式并执行查询
     */
    protected void initQuery(String q) {
        try {
            // 转换日期格式为 ISO 格式（如 "2025-08-20 12:34:56" -> "2025-08-20T12:34:56Z"）
            q = DateConverter.replaceDateTimeToISODateTime(q);
            DataImporter.QUERY_COUNT.get().incrementAndGet();
            // 调用数据源获取数据（4.x 驱动返回的迭代器）
            rowIterator = dataSource.getData(q, this.collection);
            this.query = q;
//            LOG.warn("Initialized query: {}", q);
        } catch (DataImportHandlerException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to execute query: {}", q, e);
            throw new DataImportHandlerException(SEVERE, "Query execution failed: " + e.getMessage(), e);
        }
    }

    /**
     * 获取下一行数据（全量/增量导入阶段）
     */
    @Override
    public Map<String, Object> nextRow() {
        if (rowIterator == null) {
//            LOG.warn("rowIterator is null, initializing query");
            String q = getQuery();
            if (q == null || q.trim().isEmpty()) {
                LOG.warn("No query defined for current process phase: {}", context.currentProcess());
                return null;
            }
            // 替换占位符（如 ${dih.last_index_time}）
            String resolvedQuery = context.replaceTokens(q);
            initQuery(resolvedQuery);
        }
        return getNext();
    }

    /**
     * 增量导入：获取下一个变更记录的 _id（FIND_DELTA 阶段）
     */
    @Override
    public Map<String, Object> nextModifiedRowKey() {
        if (Context.FIND_DELTA.equals(context.currentProcess())) {
            // 初始化 deltaQuery 迭代器
            if (rowIterator == null) {
                String deltaQuery = context.getEntityAttribute(DELTA_QUERY);
                if (deltaQuery == null || deltaQuery.trim().isEmpty()) {
                    throw new DataImportHandlerException(SEVERE, "deltaQuery is required for delta-import");
                }
                // 替换占位符并转换日期格式
                String resolvedQuery = context.replaceTokens(deltaQuery);
                resolvedQuery = DateConverter.replaceDateTimeToISODateTime(resolvedQuery);
                LOG.warn("Executing deltaQuery: {}", resolvedQuery);
                try {
                    rowIterator = dataSource.getData(resolvedQuery, this.collection);
                    this.query = resolvedQuery;
                } catch (Exception e) {
                    LOG.error("Failed to execute deltaQuery: {}", resolvedQuery, e);
                    throw new DataImportHandlerException(SEVERE, "deltaQuery execution failed: " + e.getMessage(), e);
                }
            }

            // 提取下一个 _id
            if (rowIterator != null && rowIterator.hasNext()) {
                Map<String, Object> deltaRow = rowIterator.next();
                Object id = deltaRow.get("_id");
                if (id != null) {
                    // 转换 _id 为字符串（兼容 ObjectId 和其他类型）
                    Map<String, Object> keyMap = new HashMap<>(1);
                    keyMap.put("_id", id.toString());
                    LOG.trace("Found delta row _id: {}", id);
                    return keyMap;
                } else {
                    LOG.warn("Skipping delta row: missing _id field");
                }
            } else {
                // 迭代结束，清空迭代器
                rowIterator = null;
                LOG.warn("No more delta rows");
            }
        }
        return null;
    }

    /**
     * 根据当前处理阶段返回对应的查询
     * - FULL_DUMP：返回 full-import 查询
     * - DELTA_DUMP：返回 delta-import 查询（根据 _id 拉取完整数据）
     */
    public String getQuery() {
        String phase = context.currentProcess();
//        LOG.warn("Current process phase: {}", phase);

        if (Context.FULL_DUMP.equals(phase)) {
            String fullQuery = context.getEntityAttribute(QUERY);
            if (fullQuery == null || fullQuery.trim().isEmpty()) {
                throw new DataImportHandlerException(SEVERE, "query is required for full-import");
            }
            return fullQuery;
        } else if (Context.DELTA_DUMP.equals(phase)) {
            String deltaImportQuery = context.getEntityAttribute(DELTA_IMPORT_QUERY);
            if (deltaImportQuery == null || deltaImportQuery.trim().isEmpty()) {
                throw new DataImportHandlerException(SEVERE, "deltaImportQuery is required for delta-import");
            }
            return deltaImportQuery;
        }
        // FIND_DELTA 阶段的查询在 nextModifiedRowKey 中单独处理
        return null;
    }

    // 常量定义
    public static final String COLLECTION = "collection";
    public static final String QUERY = "query";
    public static final String DELTA_QUERY = "deltaQuery";
    public static final String DELTA_IMPORT_QUERY = "deltaImportQuery";
}