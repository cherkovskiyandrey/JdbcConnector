package ru.sbrf.ofep.kafka.database.simpleloader;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import ru.sbrf.ofep.kafka.KafkaKey;
import ru.sbrf.ofep.kafka.config.DbConfig;
import ru.sbrf.ofep.kafka.database.Cursor;
import ru.sbrf.ofep.kafka.database.descriptions.TableDescriptor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static ru.sbrf.ofep.kafka.JdbcConnector.OFFSET_ID_NAME;
import static ru.sbrf.ofep.kafka.database.simpleloader.SeparatedBatchClient.offsetKey;
import static ru.sbrf.ofep.kafka.database.simpleloader.SeparatedBatchClient.offsetValue;

class DataMapper {
    private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
    private static final Schema VALUE_SCHEMA = Schema.BYTES_SCHEMA;
    private static final Gson GSON = new Gson();

    private final DbConfig configuration;
    private final Function<String, ResultSet, SQLException> uniqueDataIdMapper;
    private final Function<String, ResultSet, SQLException> dataTimeStampMapper;
    private final SqlTypesConverter dataType;

    private DataMapper(DbConfig configuration,
                       Function<String, ResultSet, SQLException> uniqueDataIdMapper,
                       Function<String, ResultSet, SQLException> dataTimeStampMapper,
                       SqlTypesConverter dataType) {
        this.configuration = configuration;
        this.uniqueDataIdMapper = uniqueDataIdMapper;
        this.dataTimeStampMapper = dataTimeStampMapper;
        this.dataType = dataType;
    }

    static DataMapper newInstance(DbConfig configuration,
                                  TableDescriptor tableDescr) {
        return new DataMapper(
                configuration,
                createUniqueDataIdMapper(tableDescr, configuration.getUniqueDataId(), configuration.getTableId()),
                dataTimeStampMapper(tableDescr, configuration.getDataTimestamp()),
                SqlTypesConverter.of(tableDescr.getFieldType(configuration.getTableData()))
        );
    }

    private static Function<String, ResultSet, SQLException> fieldExtractor(final TableDescriptor tableDescr, final String fieldName) {
        final SqlTypesConverter dataType = SqlTypesConverter.of(tableDescr.getFieldType(fieldName));
        return new Function<String, ResultSet, SQLException>() {
            @Override
            public String execute(ResultSet result) throws SQLException {
                return dataType.extractAsString(result, fieldName);
            }
        };
    }

    private static Function<String, ResultSet, SQLException> createUniqueDataIdMapper(final TableDescriptor tableDescr,
                                                                                      String uniqueDataId,
                                                                                      String tableId) {
        return StringUtils.isNotEmpty(uniqueDataId) ?
                fieldExtractor(tableDescr, uniqueDataId) :
                fieldExtractor(tableDescr, tableId);
    }

    private static Function<String, ResultSet, SQLException> dataTimeStampMapper(final TableDescriptor tableDescr,
                                                                                 String dataTimestamp) {
        if (StringUtils.isNotEmpty(dataTimestamp)) {
            return fieldExtractor(tableDescr, dataTimestamp);
        }
        return new Function<String, ResultSet, SQLException>() {
            @Override
            public String execute(ResultSet result) throws SQLException {
                return null;
            }
        };
    }

    SourceRecord mapToSourceRecord(ResultSet result) throws SQLException {
        final Long id = result.getLong(configuration.getTableId());
        final KafkaKey kafkaKey = KafkaKey.of(
                uniqueDataIdMapper.execute(result),
                dataTimeStampMapper.execute(result));
        final byte[] data = dataType.extractAsByteArray(result, configuration.getTableData());

        return new SourceRecord(
                offsetKey(configuration.getTableName()),
                offsetValue(id),
                configuration.getTopic(),
                KEY_SCHEMA,
                GSON.toJson(kafkaKey),
                VALUE_SCHEMA,
                data);

    }

    Cursor getLastOffsetAsCursor(List<SourceRecord> loadedData) {
        return Cursor.of((Long) loadedData.get(loadedData.size() - 1).sourceOffset().get(OFFSET_ID_NAME));
    }
}
