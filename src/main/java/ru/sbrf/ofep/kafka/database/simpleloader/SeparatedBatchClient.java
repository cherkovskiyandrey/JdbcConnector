package ru.sbrf.ofep.kafka.database.simpleloader;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbrf.ofep.kafka.Utils;
import ru.sbrf.ofep.kafka.database.Cursor;
import ru.sbrf.ofep.kafka.database.DataBaseClient;
import ru.sbrf.ofep.kafka.database.DataBaseReadStream;
import ru.sbrf.ofep.kafka.database.descriptions.TableDescriptor;
import ru.sbrf.ofep.kafka.database.descriptions.impl.SimpleTableDescriptor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static ru.sbrf.ofep.kafka.JdbcConnector.OFFSET_ID_NAME;
import static ru.sbrf.ofep.kafka.config.ConfigParam.*;


public class SeparatedBatchClient implements DataBaseClient {
    private static final Logger LOG = LoggerFactory.getLogger(SeparatedBatchClient.class);
    private static final Set<AnyTypeExtractor> SUPPORTED_DATA_TYPES = Utils.asImmutableSet(
            AnyTypeExtractor.CHAR,
            AnyTypeExtractor.CLOB,
            AnyTypeExtractor.LONGVARCHAR,
            AnyTypeExtractor.VARCHAR
    );
    private static final Set<AnyTypeExtractor> SUPPORTED_TYPES_OF_UNIQUE_DATA_ID = Utils.asImmutableSet(
            AnyTypeExtractor.CHAR,
            AnyTypeExtractor.LONGVARCHAR,
            AnyTypeExtractor.VARCHAR,
            AnyTypeExtractor.LONGVARCHAR,
            AnyTypeExtractor.BIGINT,
            AnyTypeExtractor.SMALLINT,
            AnyTypeExtractor.INTEGER
    );

    private final HikariDataSource dataSource;
    private final String uniqueDataId;
    private final String tableName;
    private final String tableId;
    private final long batchSize;
    private final String tableData;
    private final String topic;

    private SeparatedBatchClient(HikariDataSource dataSource,
                                 String uniqueDataId,
                                 String tableName,
                                 String tableId,
                                 long batchSize,
                                 String tableData,
                                 String topic) {
        this.dataSource = dataSource;
        this.uniqueDataId = uniqueDataId;
        this.tableName = tableName;
        this.tableId = tableId;
        this.batchSize = batchSize;
        this.tableData = tableData;
        this.topic = topic;
    }


    public static DataBaseClient newInstance(AbstractConfig configuration) {
        return new SeparatedBatchClient(
                createDataSource(configuration),
                configuration.getString(DATA_BASE_TABLE_DATA_UNIQUE_ID.getName()),
                configuration.getString(DATA_BASE_TABLE.getName()),
                configuration.getString(DATA_BASE_TABLE_ID.getName()),
                configuration.getLong(DATA_BASE_TABLE_BATCH_SIZE.getName()),
                configuration.getString(DATA_BASE_TABLE_CLOB.getName()),
                configuration.getString(TOPIC_CONFIG.getName())
        );
    }

    private static HikariDataSource createDataSource(AbstractConfig configuration) {
        final HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(configuration.getString(DATA_BASE_URL.getName()));
        dataSource.setUsername(configuration.getString(DATA_BASE_LOGIN.getName()));
        dataSource.setPassword(configuration.getString(DATA_BASE_PSWD.getName()));
        dataSource.setMaximumPoolSize(2);
        dataSource.setReadOnly(true);
        return dataSource;
    }

    static Map<String, ?> offsetKey(String s) {
        return Collections.singletonMap(DATA_BASE_TABLE.getName(), s);
    }

    static Map<String, ?> offsetValue(long id) {
        return Collections.singletonMap(OFFSET_ID_NAME, id);
    }

    private UniqueDataIdExtractor createUniqueDataIdExtractor(TableDescriptor tableDescr) {
        if (StringUtils.isNotEmpty(uniqueDataId)) {
            final AnyTypeExtractor dataType = AnyTypeExtractor.of(tableDescr.getFieldType(tableData));
            return new UniqueDataIdExtractor() {
                @Override
                public String extractAsString(ResultSet result) throws SQLException {
                    return dataType.extractAsString(result, uniqueDataId);
                }
            };
        }
        return new UniqueDataIdExtractor() {
            @Override
            public String extractAsString(ResultSet result) throws SQLException {
                return Long.toString(result.getLong(tableId));
            }
        };
    }

    @Override
    public DataBaseReadStream createNewStream(Cursor beginCursor) throws SQLException {
        final TableDescriptor tableDescriptor = SimpleTableDescriptor.instanceFor(dataSource, tableName);
        checkRestrictions(tableDescriptor);

        return new SeparatedBatchReadStream(dataSource,
                tableDescriptor.getVendor().getQueryBuilder(),
                createUniqueDataIdExtractor(tableDescriptor),
                extractDataType(tableDescriptor),
                tableName,
                tableId,
                batchSize,
                tableData,
                topic,
                beginCursor,
                null
        );
    }

    private AnyTypeExtractor extractDataType(TableDescriptor tableDescr) {
        return AnyTypeExtractor.of(tableDescr.getFieldType(tableData));
    }

    private void checkRestrictions(TableDescriptor tableDescr) {
        if (!tableDescr.isTableExists()) {
            throw new IllegalArgumentException(format("Table %s does not exists!", tableName));
        }

        if (!tableDescr.isPrimaryKey(tableId) || !tableDescr.isAutoIncrement(tableId)) {
            throw new IllegalArgumentException(format("For table %s, %s is not primary key or is not autoincrement!",
                    tableName, tableId));
        }

        if (StringUtils.isNotEmpty(uniqueDataId)) {
            int fieldFormat = tableDescr.getFieldType(uniqueDataId);
            final AnyTypeExtractor dataType = AnyTypeExtractor.of(fieldFormat);
            if (dataType == null || !SUPPORTED_TYPES_OF_UNIQUE_DATA_ID.contains(dataType)) {
                throw new IllegalArgumentException(format("Unsupported format %d for unique data id field %s for table %s!", fieldFormat,
                        tableData, tableName));
            }
        }

        int fieldFormat = tableDescr.getFieldType(tableData);
        final AnyTypeExtractor dataType = AnyTypeExtractor.of(fieldFormat);
        if (dataType == null || !SUPPORTED_DATA_TYPES.contains(dataType)) {
            throw new IllegalArgumentException(format("Unsupported format %d for data field %s for table %s!", fieldFormat,
                    tableData, tableName));
        }
    }

    @Override
    public Cursor newCursorFromOffsetStorage(OffsetStorageReader offsetStorageReader) {
        Map<String, Object> offset = offsetStorageReader.offset(offsetKey(tableName));
        if (offset == null) {
            return Cursor.UNDEFINED;
        }
        final Object lastRecordedOffset = offset.get(OFFSET_ID_NAME);
        if (lastRecordedOffset != null && lastRecordedOffset instanceof Long) {
            return Cursor.of((Long) lastRecordedOffset);
        }
        return Cursor.UNDEFINED;
    }

    @Override
    public void close() throws Exception {
        dataSource.close();
    }

}
