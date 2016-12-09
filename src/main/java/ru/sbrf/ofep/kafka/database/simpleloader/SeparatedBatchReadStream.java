package ru.sbrf.ofep.kafka.database.simpleloader;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbrf.ofep.kafka.database.Cursor;
import ru.sbrf.ofep.kafka.database.DataBaseReadStream;
import ru.sbrf.ofep.kafka.database.descriptions.StatTableInfo;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.String.format;
import static ru.sbrf.ofep.kafka.database.simpleloader.SeparatedBatchClient.offsetKey;
import static ru.sbrf.ofep.kafka.database.simpleloader.SeparatedBatchClient.offsetValue;

public class SeparatedBatchReadStream implements DataBaseReadStream {
    private static final Logger LOG = LoggerFactory.getLogger(SeparatedBatchReadStream.class);

    public interface QueryBuilder {
        PreparedStatement buildQuery(Connection conn, String tableName, String tableId, long from, long batchSize) throws SQLException;
    }

    private static final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
    private static final Schema VALUE_SCHEMA = Schema.BYTES_SCHEMA;

    private final DataSource dataSource;
    private final QueryBuilder queryBuilder;
    private final UniqueDataIdExtractor uniqueDataIdExtractor;
    private final AnyTypeExtractor dataType;
    private final String tableName;
    private final String tableId;
    private final long batchSize;
    private final String tableData;
    private final String topic;

    private Cursor currentCursor;
    private StatTableInfo statTableInfo;

    SeparatedBatchReadStream(DataSource dataSource,
                                    QueryBuilder queryBuilder,
                                    UniqueDataIdExtractor uniqueDataIdExtractor,
                                    AnyTypeExtractor dataType,
                                    String tableName,
                                    String tableId,
                                    long batchSize,
                                    String tableData,
                                    String topic,
                                    Cursor currentCursor,
                                    StatTableInfo statTableInfo) {
        this.dataSource = dataSource;
        this.queryBuilder = queryBuilder;
        this.uniqueDataIdExtractor = uniqueDataIdExtractor;
        this.dataType = dataType;
        this.tableName = tableName;
        this.tableId = tableId;
        this.batchSize = batchSize;
        this.tableData = tableData;
        this.topic = topic;
        this.currentCursor = currentCursor;
        this.statTableInfo = statTableInfo;
    }

    @Override
    public List<SourceRecord> read() throws SQLException {
        updateTableInfo();
        LOG.trace("Current table info: " + statTableInfo);

        if (statTableInfo.isTableEmpty()) {
            return Collections.emptyList();
        }

        rewindCursorIfNeed();
        if (statTableInfo.getMaxId() < currentCursor.getInnerOffset()) {
            return Collections.emptyList();
        }

        return readImpl();
    }

    private List<SourceRecord> readImpl() throws SQLException {
        final List<SourceRecord> container = new ArrayList<>((int) batchSize);
        final Cursor loadedCursor = scopedLoadData(container);

        if(!container.isEmpty()) {
            final Cursor old = currentCursor;
            currentCursor = loadedCursor.next();
            LOG.trace(format("Cursor has been incremented from %s to %s", old, currentCursor));
        }
        return container;
    }

    private Cursor scopedLoadData(List<SourceRecord> container) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement st = queryBuilder.buildQuery(conn, tableName, tableId, currentCursor.getInnerOffset(), batchSize)) {

            LOG.trace(format("Try to execute query with params: table[%s], from[%d], to[%d]",
                    tableName, currentCursor.getInnerOffset(), currentCursor.getInnerOffset() + batchSize));

            try (ResultSet result = st.executeQuery()) {
                return loadDataHelper(result, container);
            }
        }
    }

    private Cursor loadDataHelper(ResultSet result, List<SourceRecord> container) throws SQLException {
        Cursor id = currentCursor;
        while (result.next()) {
            id = Cursor.of(result.getLong(tableId));
            final String dataUniqueId = uniqueDataIdExtractor.extractAsString(result);
            final byte[] data = dataType.extractAsByteArray(result, tableData);

            container.add(new SourceRecord(
                    offsetKey(tableName),
                    offsetValue(id.getInnerOffset()),
                    topic,
                    KEY_SCHEMA, dataUniqueId,
                    VALUE_SCHEMA, data));
        }
        return id;
    }

    private void rewindCursorIfNeed() {
        final Cursor old = currentCursor;
        currentCursor = Cursor.of(Math.max(currentCursor.getInnerOffset(), statTableInfo.getMinId()));

        if(!old.equals(currentCursor)) {
            LOG.trace(format("Cursor has been moved from %s to %s", old, currentCursor));
        }
    }

    private void updateTableInfo() throws SQLException {
        statTableInfo = StatTableInfo.of(dataSource, tableName, tableId);
    }

    @Override
    public Cursor currentCursor() {
        return currentCursor;
    }

    @Override
    public void close() throws Exception {
    }
}
