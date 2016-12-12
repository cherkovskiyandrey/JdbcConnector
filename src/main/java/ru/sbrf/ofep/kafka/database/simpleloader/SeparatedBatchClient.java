package ru.sbrf.ofep.kafka.database.simpleloader;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import ru.sbrf.ofep.kafka.Utils;
import ru.sbrf.ofep.kafka.config.DbConfig;
import ru.sbrf.ofep.kafka.database.Cursor;
import ru.sbrf.ofep.kafka.database.DataBaseClient;
import ru.sbrf.ofep.kafka.database.DataBaseReadStream;
import ru.sbrf.ofep.kafka.database.descriptions.TableDescriptor;
import ru.sbrf.ofep.kafka.database.descriptions.impl.SimpleTableDescriptor;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static ru.sbrf.ofep.kafka.JdbcConnector.OFFSET_ID_NAME;
import static ru.sbrf.ofep.kafka.config.ConfigParam.DATA_BASE_TABLE;


public class SeparatedBatchClient implements DataBaseClient {
    private static final Set<SqlTypesConverter> SUPPORTED_DATA_TYPES = Utils.asImmutableSet(
            SqlTypesConverter.CHAR,
            SqlTypesConverter.CLOB,
            SqlTypesConverter.LONGVARCHAR,
            SqlTypesConverter.VARCHAR
    );
    private static final Set<SqlTypesConverter> SUPPORTED_TYPES_OF_UNIQUE_DATA_ID = Utils.asImmutableSet(
            SqlTypesConverter.CHAR,
            SqlTypesConverter.LONGVARCHAR,
            SqlTypesConverter.VARCHAR,
            SqlTypesConverter.LONGVARCHAR,
            SqlTypesConverter.BIGINT,
            SqlTypesConverter.SMALLINT,
            SqlTypesConverter.INTEGER
    );
    private static final Set<SqlTypesConverter> SUPPORTED_TYPES_OF_TIMESTAMP = Utils.asImmutableSet(
            SqlTypesConverter.DATE,
            SqlTypesConverter.TIMESTAMP,
            SqlTypesConverter.TIMESTAMPTZ
    );

    private final HikariDataSource dataSource;
    private final DbConfig configuration;

    private SeparatedBatchClient(HikariDataSource dataSource, DbConfig configuration) {
        this.dataSource = dataSource;
        this.configuration = configuration;
    }


    public static DataBaseClient newInstance(DbConfig configuration) {
        return new SeparatedBatchClient(createDataSource(configuration), configuration);
    }

    private static HikariDataSource createDataSource(DbConfig configuration) {
        final HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(configuration.getDbUrl());
        dataSource.setUsername(configuration.getDbLogin());
        dataSource.setPassword(configuration.getDbPswd());
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

    @Override
    public DataBaseReadStream createNewStream(Cursor beginCursor) throws SQLException {
        final TableDescriptor tableDescriptor = SimpleTableDescriptor.instanceFor(dataSource, configuration.getTableName());
        checkRestrictions(tableDescriptor);

        return new SeparatedBatchReadStream(dataSource,
                tableDescriptor.getVendor().getQueryBuilder(),
                configuration,
                createDataMapper(tableDescriptor),
                beginCursor
        );
    }

    private DataMapper createDataMapper(TableDescriptor tableDescriptor) {
        return DataMapper.newInstance(configuration, tableDescriptor);
    }

    private void checkRestrictions(TableDescriptor tableDescr) {
        checkTableExists(tableDescr);
        checkPrimaryKey(tableDescr);
        checkUniqueDataId(tableDescr);
        checkDataTimestamp(tableDescr);
        checkTableData(tableDescr);
    }

    private void checkDataTimestamp(TableDescriptor tableDescr) {
        if (StringUtils.isNotEmpty(configuration.getDataTimestamp())) {
            int fieldFormat = tableDescr.getFieldType(configuration.getDataTimestamp());
            final SqlTypesConverter dataType = SqlTypesConverter.of(fieldFormat);
            if (dataType == null || !SUPPORTED_TYPES_OF_TIMESTAMP.contains(dataType)) {
                throw new IllegalArgumentException(format("Unsupported format %d for timestamp field %s for table %s!", fieldFormat,
                        configuration.getDataTimestamp(), configuration.getTableName()));
            }
        }
    }

    private void checkTableData(TableDescriptor tableDescr) {
        int fieldFormat = tableDescr.getFieldType(configuration.getTableData());
        final SqlTypesConverter dataType = SqlTypesConverter.of(fieldFormat);
        if (dataType == null || !SUPPORTED_DATA_TYPES.contains(dataType)) {
            throw new IllegalArgumentException(format("Unsupported format %d for data field %s for table %s!", fieldFormat,
                    configuration.getTableData(), configuration.getTableName()));
        }
    }

    private void checkUniqueDataId(TableDescriptor tableDescr) {
        if (StringUtils.isNotEmpty(configuration.getUniqueDataId())) {
            int fieldFormat = tableDescr.getFieldType(configuration.getUniqueDataId());
            final SqlTypesConverter dataType = SqlTypesConverter.of(fieldFormat);
            if (dataType == null || !SUPPORTED_TYPES_OF_UNIQUE_DATA_ID.contains(dataType)) {
                throw new IllegalArgumentException(format("Unsupported format %d for unique data id field %s for table %s!", fieldFormat,
                        configuration.getUniqueDataId(), configuration.getTableName()));
            }
        }
    }

    private void checkPrimaryKey(TableDescriptor tableDescr) {
        if (!tableDescr.isPrimaryKey(configuration.getTableId()) ||
                !tableDescr.isAutoIncrement(configuration.getTableId())) {
            throw new IllegalArgumentException(format("For table %s, %s is not primary key or is not autoincrement!",
                    configuration.getTableName(), configuration.getTableId()));
        }
    }

    private void checkTableExists(TableDescriptor tableDescr) {
        if (!tableDescr.isTableExists()) {
            throw new IllegalArgumentException(format("Table %s does not exists!", configuration.getTableName()));
        }
    }

    @Override
    public Cursor newCursorFromOffsetStorage(OffsetStorageReader offsetStorageReader) {
        Map<String, Object> offset = offsetStorageReader.offset(offsetKey(configuration.getTableName()));
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
