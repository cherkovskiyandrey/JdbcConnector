package ru.sbrf.ofep.kafka;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static ru.sbrf.ofep.kafka.JdbcConnector.OFFSET_ID_NAME;
import static ru.sbrf.ofep.kafka.config.ConfigParam.*;

public class JdbcTaskTest extends AbstractTest {

    private void insertData(int from, int count) throws SQLException {
        insertDataHelper(from, count, 1, "hello world", "hello clob world");
    }

    private void insertDataHelper(int from, int count, long uuid, String data, String clobData) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement st = conn.prepareStatement(
                    "insert into test (id, uuid, data, clob_data) " +
                            "values (?, ?, ?, ?)")) {
                for (int k = from; k < from + count; ++k) {
                    st.setInt(1, k);
                    st.setLong(2, uuid);
                    st.setString(3, data);
                    st.setString(4, clobData);
                    st.addBatch();
                }
                st.executeBatch();
            }
        }
    }

    private Map<String, String> createSettings(String clob) {
        Map<String, String> settings = new HashMap<>();

        settings.put(TOPIC_CONFIG.getName(), "test_topic");
        settings.put(DATA_BASE_URL.getName(), "jdbc:h2:mem:dbTest;MODE=Oracle;");
        settings.put(DATA_BASE_LOGIN.getName(), "sa");
        settings.put(DATA_BASE_PSWD.getName(), "");
        settings.put(DATA_BASE_TABLE.getName(), "test");
        settings.put(DATA_BASE_TABLE_ID.getName(), "id");
        settings.put(DATA_BASE_TABLE_CLOB.getName(), clob);
        settings.put(DATA_BASE_TABLE_BATCH_SIZE.getName(), "10000");
        settings.put(DATA_BASE_POLL_INTERVAL.getName(), "1");

        return settings;
    }

    @Test
    public void loadEmptyTableTest() throws Exception {
        try (final SourceBundle sourceBundle = new SourceBundle(0, "data")) {
            List<SourceRecord> result = sourceBundle.getJdbcTask().poll();
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void loadOneElementTableTest() throws Exception {
        try (final SourceBundle sourceBundle = new SourceBundle(0, "data")) {
            insertData(1, 1);

            List<SourceRecord> result = sourceBundle.getJdbcTask().poll();

            assertEquals(1, result.size());
            assertEquals(1l, result.get(0).sourceOffset().get(OFFSET_ID_NAME));

            result = sourceBundle.getJdbcTask().poll();
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void loadAfterRestartTest() throws Exception {
        long currentId = 0;
        try (final SourceBundle sourceBundle = new SourceBundle(0, "data")) {
            insertData(1, 1);

            List<SourceRecord> result = sourceBundle.getJdbcTask().poll();

            assertEquals(1, result.size());
            assertEquals(1l, result.get(0).sourceOffset().get(OFFSET_ID_NAME));

            currentId = (Long) result.get(0).sourceOffset().get(OFFSET_ID_NAME);
        }

        try (final SourceBundle sourceBundle = new SourceBundle(currentId, "data")) {
            List<SourceRecord> result = sourceBundle.getJdbcTask().poll();
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void pollFromBeginTest() throws Exception {
        try (final SourceBundle sourceBundle = new SourceBundle(0, "data")) {
            insertData(1, 100_000);

            for (int l = 0; l < 10; ++l) {
                List<SourceRecord> result = sourceBundle.getJdbcTask().poll();

                assertEquals(10000, result.size());
                assertEquals(1l + 10000 * l, result.get(0).sourceOffset().get(OFFSET_ID_NAME));
                assertEquals(10000l * (l + 1), result.get(9999).sourceOffset().get(OFFSET_ID_NAME));

                when(sourceBundle.getOffset().offset(anyMap()))
                        .thenReturn(Collections.singletonMap(OFFSET_ID_NAME,
                                result.get(9999).sourceOffset().get(OFFSET_ID_NAME)));
            }
        }
    }

    @Test
    public void pollMoreThanExistsTest() throws Exception {
        try (final SourceBundle sourceBundle = new SourceBundle(0, "data")) {
            insertData(1, 100);

            List<SourceRecord> result = sourceBundle.getJdbcTask().poll();

            assertEquals(100, result.size());
            assertEquals(1l, result.get(0).sourceOffset().get(OFFSET_ID_NAME));
            assertEquals(100l, result.get(99).sourceOffset().get(OFFSET_ID_NAME));

            result = sourceBundle.getJdbcTask().poll();
            assertTrue(result.isEmpty());
        }
    }

    @Test
    public void pollMoreThanExistsCircleTest() throws Exception {
        try (final SourceBundle sourceBundle = new SourceBundle(0, "data")) {

            for (int l = 0; l < 10; ++l) {
                insertData(1 + (100 * l), 100);
                List<SourceRecord> result = sourceBundle.getJdbcTask().poll();

                assertEquals(100, result.size());
                assertEquals(1l + 100 * l, result.get(0).sourceOffset().get(OFFSET_ID_NAME));
                assertEquals(100l * (l + 1), result.get(99).sourceOffset().get(OFFSET_ID_NAME));

                when(sourceBundle.getOffset().offset(anyMap()))
                        .thenReturn(Collections.singletonMap(OFFSET_ID_NAME,
                                result.get(99).sourceOffset().get(OFFSET_ID_NAME)));
            }
        }
    }

    @Test
    public void loadOneVarcharElementTest() throws Exception {
        try (final SourceBundle sourceBundle = new SourceBundle(0, "data")) {
            insertData(1, 1);

            List<SourceRecord> result = sourceBundle.getJdbcTask().poll();

            assertEquals(1, result.size());
            assertEquals(1l, result.get(0).sourceOffset().get(OFFSET_ID_NAME));
            assertEquals(byte[].class, result.get(0).value().getClass());
            assertEquals("hello world", new String((byte[]) result.get(0).value(), "UTF-8"));
        }
    }

    @Test
    public void loadOneClobElementTest() throws Exception {
        try (final SourceBundle sourceBundle = new SourceBundle(0, "clob_data")) {
            insertData(1, 1);

            List<SourceRecord> result = sourceBundle.getJdbcTask().poll();

            assertEquals(1, result.size());
            assertEquals(1l, result.get(0).sourceOffset().get(OFFSET_ID_NAME));
            assertEquals(byte[].class, result.get(0).value().getClass());
            assertEquals("hello clob world", new String((byte[]) result.get(0).value(), "UTF-8"));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidClobParamTest() throws Exception {
        try (final SourceBundle sourceBundle = new SourceBundle(0, "not_exists_field")) {
            insertData(1, 1);
            sourceBundle.getJdbcTask().poll();
        }
    }

    @Test(expected = ConfigException.class)
    public void invalidBatchSizeParamTest() throws Exception {
        Map<String, String> settings = createSettings("data");
        settings.put(DATA_BASE_TABLE_BATCH_SIZE.getName(), "-10000");
        try (final SourceBundle sourceBundle = new SourceBundle(settings, 0)) {
            insertData(1, 1);
            sourceBundle.getJdbcTask().poll();
        }
    }

    @Test
    public void nullStringFieldDataTest() throws Exception {
        try (final SourceBundle sourceBundle = new SourceBundle(0, "data")) {
            insertDataHelper(1, 1, 1, null, null);

            List<SourceRecord> result = sourceBundle.getJdbcTask().poll();

            assertEquals(1, result.size());
            assertEquals(0, ((byte[]) result.get(0).value()).length);
            assertEquals(1l, result.get(0).sourceOffset().get(OFFSET_ID_NAME));
        }
    }

    @Test
    public void nullClobFieldDataTest() throws Exception {
        try (final SourceBundle sourceBundle = new SourceBundle(0, "clob_data")) {
            insertDataHelper(1, 1, 1,null, null);

            List<SourceRecord> result = sourceBundle.getJdbcTask().poll();

            assertEquals(1, result.size());
            assertEquals(0, ((byte[]) result.get(0).value()).length);
            assertEquals(1l, result.get(0).sourceOffset().get(OFFSET_ID_NAME));
        }
    }

    @Test
    public void uniqueDataIdNotExistsTest() throws Exception {
        try (final SourceBundle sourceBundle = new SourceBundle(0, "data")) {
            insertDataHelper(1, 1, 999,null, null);

            List<SourceRecord> result = sourceBundle.getJdbcTask().poll();

            assertEquals(1, result.size());
            assertEquals("1", result.get(0).key());
            assertEquals(0, ((byte[]) result.get(0).value()).length);
            assertEquals(1l, result.get(0).sourceOffset().get(OFFSET_ID_NAME));
        }
    }

    @Test
    public void uniqueDataIdExistsTest() throws Exception {
        Map<String, String> settings = createSettings("data");
        settings.put(DATA_BASE_TABLE_DATA_UNIQUE_ID.getName(), "uuid");
        try (final SourceBundle sourceBundle = new SourceBundle(settings, 0)) {
            insertDataHelper(1, 1, 999,null, null);

            List<SourceRecord> result = sourceBundle.getJdbcTask().poll();

            assertEquals(1, result.size());
            assertEquals("999", result.get(0).key());
            assertEquals(0, ((byte[]) result.get(0).value()).length);
            assertEquals(1l, result.get(0).sourceOffset().get(OFFSET_ID_NAME));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void uniqueDataIdExistsAndIncorrectedTest() throws Exception {
        Map<String, String> settings = createSettings("data");
        settings.put(DATA_BASE_TABLE_DATA_UNIQUE_ID.getName(), "unknown_uuid");
        try (final SourceBundle sourceBundle = new SourceBundle(settings, 0)) {
            insertDataHelper(1, 1, 999,null, null);
            sourceBundle.getJdbcTask().poll();
        }
    }

    private class SourceBundle implements AutoCloseable {
        private final Map<String, String> settings;
        private final JdbcTask jdbcTask;
        private final SourceTaskContext ctx;
        private final OffsetStorageReader offset;

        SourceBundle(Map<String, String> settings, long beginOffset) {
            this.settings = settings;
            jdbcTask = new JdbcTask();
            ctx = mock(SourceTaskContext.class);
            offset = mock(OffsetStorageReader.class);

            when(ctx.offsetStorageReader()).thenReturn(offset);
            when(offset.offset(anyMap()))
                    .thenReturn(Collections.singletonMap(OFFSET_ID_NAME, beginOffset));

            jdbcTask.initialize(ctx);
            jdbcTask.start(settings);
        }

        SourceBundle(long beginOffset, String clob) {
            settings = createSettings(clob);
            jdbcTask = new JdbcTask();
            ctx = mock(SourceTaskContext.class);
            offset = mock(OffsetStorageReader.class);

            when(ctx.offsetStorageReader()).thenReturn(offset);
            when(offset.offset(anyMap()))
                    .thenReturn(Collections.singletonMap(OFFSET_ID_NAME, beginOffset));

            jdbcTask.initialize(ctx);
            jdbcTask.start(settings);
        }

        JdbcTask getJdbcTask() {
            return jdbcTask;
        }

        OffsetStorageReader getOffset() {
            return offset;
        }

        Map<String, String> getSettings() {
            return settings;
        }

        @Override
        public void close() throws Exception {
            jdbcTask.stop();
        }
    }

}






