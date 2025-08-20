/**
 * Solr MongoDB data import handler
 */
package org.apache.solr.handler.dataimport;

import com.mongodb.*;
import com.mongodb.client.*;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

/**
 * Solr MongoDB data source (适配 MongoDB 4.x 驱动)
 */
public class MongoDataSource extends DataSource<Iterator<Map<String, Object>>> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDataSource.class);

    private MongoCollection<Document> mongoCollection; // 明确泛型类型为 Document
    private MongoDatabase mongoDb;
    private MongoClient mongoClient;
    private MongoCursor<Document> mongoCursor; // 明确泛型类型为 Document
    private String mapMongoFields;

    /**
     * 初始化 MongoDB 连接
     * 适配点：使用 MongoClients.create() 替代旧版 MongoClient 构造方法
     */
    @Override
    public void init(Context context, Properties initProps) {
        String databaseName = initProps.getProperty(DATABASE);
        String host = initProps.getProperty(HOST, "localhost");
        String port = initProps.getProperty(PORT, "27017");
        String username = initProps.getProperty(USERNAME);
        String password = initProps.getProperty(PASSWORD);
        mapMongoFields = initProps.getProperty(MAP_MONGO_FIELDS, "true");

        // 解析主机和端口（支持副本集）
        List<ServerAddress> seeds = new ArrayList<>();
        String[] hosts = host.split(",");
        String[] ports = port.split(",");

        for (int i = 0; i < hosts.length; i++) {
            int seedPort = (hosts.length == ports.length) ? Integer.parseInt(ports[i]) : Integer.parseInt(ports[0]);
            seeds.add(new ServerAddress(hosts[i], seedPort));
        }

        if (databaseName == null || databaseName.trim().isEmpty()) {
            throw new DataImportHandlerException(SEVERE, "MongoDB database name must be specified");
        }

        try {
            // 构建 MongoDB 客户端（4.x 推荐使用 MongoClients.create()）
            MongoClientSettings.Builder clientSettingsBuilder = MongoClientSettings.builder()
                    .applyToClusterSettings(builder -> builder.hosts(seeds));

            // 添加认证信息（如果提供了用户名密码）
            if (username != null && !username.isEmpty()) {
                MongoCredential credential = MongoCredential.createCredential(
                        username,
                        databaseName,
                        password.toCharArray()
                );
                clientSettingsBuilder.credential(credential);
            }

            // 设置读取偏好（优先从从节点读取）
            clientSettingsBuilder.readPreference(ReadPreference.secondaryPreferred());

            this.mongoClient = MongoClients.create(clientSettingsBuilder.build());
            this.mongoDb = mongoClient.getDatabase(databaseName);
            LOG.info("Successfully connected to MongoDB database: {}", databaseName);

        } catch (Exception e) {
            LOG.error("Failed to initialize MongoDB connection", e);
            throw new DataImportHandlerException(SEVERE, "Unable to connect to MongoDB: " + e.getMessage());
        }
    }

    /**
     * 执行查询并返回结果迭代器
     * 适配点：使用 Document.parse() 替代 JSON.parse()，移除 BasicDBObject 依赖
     */
    @Override
    public Iterator<Map<String, Object>> getData(String query) {
        try {
            // 4.x 驱动使用 Document.parse() 解析查询字符串，替代旧版 JSON.parse()
            Document queryDoc = Document.parse(query);
            LOG.warn("Executing MongoDB query: {}", queryDoc.toJson());

            // 获取查询游标（明确泛型为 Document）
            mongoCursor = this.mongoCollection.find(queryDoc).iterator();
            return new ResultSetIterator(mongoCursor).getIterator();

        } catch (Exception e) {
            LOG.error("Failed to execute query: {}", query, e);
            throw new DataImportHandlerException(SEVERE, "Query execution failed: " + e.getMessage());
        }
    }

    /**
     * 指定集合执行查询
     */
    public Iterator<Map<String, Object>> getData(String query, String collection) {
        this.mongoCollection = this.mongoDb.getCollection(collection);
        return getData(query);
    }

    /**
     * 结果集迭代器：处理 MongoDB 文档到 Solr 字段的映射
     */
    private class ResultSetIterator {
        private MongoCursor<Document> mongoCursor;
        private final Iterator<Map<String, Object>> resultSetIterator;

        public ResultSetIterator(MongoCursor<Document> cursor) {
            this.mongoCursor = cursor;
            this.resultSetIterator = new Iterator<Map<String, Object>>() {
                @Override
                public boolean hasNext() {
                    return hasNextRecord();
                }

                @Override
                public Map<String, Object> next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException("No more records available");
                    }
                    return getNextRow();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Remove operation is not supported");
                }
            };
        }

        public Iterator<Map<String, Object>> getIterator() {
            return resultSetIterator;
        }

        /**
         * 将 MongoDB Document 转换为 Solr 字段映射
         */
        private Map<String, Object> getNextRow() {
            Document document = mongoCursor.next();
            LOG.warn("row: {}", document.toJson());
            Map<String, Object> result = new HashMap<>();

            Set<String> keys = "true".equals(mapMongoFields)
                    ? getDocumentKeys(document, null)
                    : document.keySet();

            for (String key : keys) {
                Object value = getDocumentFieldValue(document, key);
                value = serializeObject(value); // 处理特殊类型（如 ObjectId、嵌套文档）
                result.put(key, value);
            }

            return result;
        }

        /**
         * 序列化 MongoDB 特殊类型为 Solr 兼容类型
         */
        private Object serializeObject(Object object) {
            if (object == null) {
                return null;
            }
            // 处理数组
            if (object instanceof List<?>) {
                List<Object> serializedList = new ArrayList<>();
                for (Object item : (List<?>) object) {
                    serializedList.add(serializeObject(item));
                }
                return serializedList;
            }
            // 处理嵌套文档
            if (object instanceof Document) {
                return ((Document) object).toJson(); // 转换为 JSON 字符串
            }
            // 处理 ObjectId
            if (object instanceof ObjectId) {
                return object.toString(); // 转换为字符串 ID
            }
            // 其他类型直接返回
            return object;
        }

        /**
         * 递归获取文档中所有字段（支持嵌套字段，用点分隔）
         */
        private Set<String> getDocumentKeys(Document doc, String parentKey) {
            Set<String> keys = new HashSet<>();
            for (String docKey : doc.keySet()) {
                Object value = doc.get(docKey);
                String currentKey = (parentKey == null) ? docKey : parentKey + "." + docKey;

                // 处理嵌套文档
                if (value instanceof Document) {
                    keys.addAll(getDocumentKeys((Document) value, currentKey));
                }
                // 处理数组
                else if (value instanceof List<?>) {
                    boolean hasNestedObjects = false;
                    for (Object item : (List<?>) value) {
                        if (item instanceof Document || item instanceof List<?>) {
                            hasNestedObjects = true;
                            break;
                        }
                    }
                    if (hasNestedObjects) {
                        // 嵌套对象数组：生成带索引的字段名（如 array.0.field）
                        for (int i = 0; i < ((List<?>) value).size(); i++) {
                            Object item = ((List<?>) value).get(i);
                            if (item instanceof Document) {
                                keys.addAll(getDocumentKeys((Document) item, currentKey + "." + i));
                            } else if (item instanceof List<?>) {
                                // 处理二维数组（简化处理）
                                keys.add(currentKey + "." + i);
                            }
                        }
                    } else {
                        // 简单类型数组：直接使用字段名
                        keys.add(currentKey);
                    }
                }
                // 普通字段
                else {
                    keys.add(currentKey);
                }
            }
            return keys;
        }

        /**
         * 根据字段路径（如 parent.child）获取文档中的值
         */
        private Object getDocumentFieldValue(Document doc, String fieldPath) {
            String[] parts = fieldPath.split("\\.");
            Object value = doc.get(parts[0]);

            for (int i = 1; i < parts.length; i++) {
                if (value == null) {
                    break;
                }
                // 处理数组索引
                if (value instanceof List<?>) {
                    try {
                        int index = Integer.parseInt(parts[i]);
                        value = ((List<?>) value).get(index);
                    } catch (NumberFormatException | IndexOutOfBoundsException e) {
                        LOG.warn("Invalid array index in field path: {}", fieldPath, e);
                        value = null;
                    }
                }
                // 处理嵌套文档
                else if (value instanceof Document) {
                    value = ((Document) value).get(parts[i]);
                } else {
                    // 路径不匹配（如非文档类型有子字段）
                    value = null;
                }
            }
            return value;
        }

        /**
         * 检查是否有下一条记录
         */
        private boolean hasNextRecord() {
            if (mongoCursor == null) {
                return false;
            }
            try {
                if (mongoCursor.hasNext()) {
                    return true;
                } else {
                    closeCursor();
                    return false;
                }
            } catch (MongoException e) {
                closeCursor();
                wrapAndThrow(SEVERE, e);
                return false;
            }
        }

        /**
         * 关闭游标释放资源
         */
        private void closeCursor() {
            try {
                if (mongoCursor != null) {
                    mongoCursor.close();
                }
            } catch (Exception e) {
                LOG.warn("Error closing MongoDB cursor", e);
            } finally {
                mongoCursor = null;
            }
        }
    }

    /**
     * 关闭数据源连接
     */
    @Override
    public void close() {
        try {
            if (mongoCursor != null) {
                mongoCursor.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing MongoDB cursor", e);
        } finally {
            mongoCursor = null;
        }

        try {
            if (mongoClient != null) {
                mongoClient.close();
                LOG.info("MongoDB client closed");
            }
        } catch (Exception e) {
            LOG.warn("Error closing MongoDB client", e);
        } finally {
            mongoClient = null;
        }
    }

    // 配置参数常量
    public static final String DATABASE = "database";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String MAP_MONGO_FIELDS = "mapMongoFields";
}