package ru.sbrf.ofep.kafka.database.simpleloader;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbrf.ofep.kafka.config.DbConfig;
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

public class SeparatedBatchReadStream implements DataBaseReadStream {
    private static final Logger LOG = LoggerFactory.getLogger(SeparatedBatchReadStream.class);

    public interface QueryBuilder {
        PreparedStatement buildQuery(Connection conn, String tableName, String tableId, long from, long batchSize)
                throws SQLException;
    }

    private final DataSource dataSource;
    private final QueryBuilder queryBuilder;
    private final DbConfig configuration;
    private final DataMapper dataMapper;

    private Cursor currentCursor;
    private StatTableInfo statTableInfo;

    SeparatedBatchReadStream(DataSource dataSource,
                             QueryBuilder queryBuilder,
                             DbConfig configuration,
                             DataMapper dataMapper,
                             Cursor currentCursor) {
        this.dataSource = dataSource;
        this.queryBuilder = queryBuilder;
        this.configuration = configuration;
        this.dataMapper = dataMapper;
        this.currentCursor = currentCursor;
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
        final List<SourceRecord> loadedData = scopedLoadData();

        if(!loadedData.isEmpty()) {
            final Cursor old = currentCursor;
            currentCursor = dataMapper.getLastOffsetAsCursor(loadedData).next();
            LOG.trace(format("Cursor has been incremented from %s to %s", old, currentCursor));
        }
        return loadedData;
    }

    private List<SourceRecord> scopedLoadData() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement st = queryBuilder.buildQuery(conn, configuration.getTableName(),
                     configuration.getTableId(), currentCursor.getInnerOffset(), configuration.getBatchSize())) {

            LOG.trace(format("Try to execute query with params: table[%s], from[%d], to[%d]",
                    configuration.getTableName(), currentCursor.getInnerOffset(),
                    currentCursor.getInnerOffset() + configuration.getBatchSize()));

            try (ResultSet result = st.executeQuery()) {
                return loadDataHelper(result);
            }
        }
    }

    private List<SourceRecord> loadDataHelper(ResultSet result) throws SQLException {
        final List<SourceRecord> container = new ArrayList<>((int) configuration.getBatchSize());
        while (result.next()) {
            container.add(dataMapper.mapToSourceRecord(result));
        }
        return container;
    }

    private void rewindCursorIfNeed() {
        final Cursor old = currentCursor;
        currentCursor = Cursor.of(Math.max(currentCursor.getInnerOffset(), statTableInfo.getMinId()));

        if(!old.equals(currentCursor)) {
            LOG.trace(format("Cursor has been moved from %s to %s", old, currentCursor));
        }
    }

    private void updateTableInfo() throws SQLException {
        statTableInfo = StatTableInfo.of(dataSource, configuration.getTableName(), configuration.getTableId());
    }

    @Override
    public Cursor currentCursor() {
        return currentCursor;
    }

    @Override
    public void close() throws Exception {
    }
}
