package ru.sbrf.ofep.kafka.database.descriptions;

import org.junit.Test;
import ru.sbrf.ofep.kafka.AbstractTest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class StatTableInfoTest extends AbstractTest {

    private void insertData(int i) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            for(int k = 0; k < i; ++k) {
                try (PreparedStatement st = conn.prepareStatement("insert into test (id, data) values (?, 'hello world')")) {
                    st.setInt(1, k + 1);
                    st.execute();
                }
            }
        }
    }

    @Test(expected = SQLException.class)
    public void ofForNotExistsTable() throws Exception {
        StatTableInfo.of(dataSource, "not_exist_table", "id");
    }

    @Test
    public void ofForEmptyTable() throws Exception {
        StatTableInfo m = StatTableInfo.of(dataSource, "test", "id");
        assertTrue(m.isTableEmpty());
        assertEquals(-1, m.getMaxId());
        assertEquals(-1, m.getMinId());
    }

    @Test
    public void ofForOneElementTable() throws Exception {
        insertData(1);
        StatTableInfo m = StatTableInfo.of(dataSource, "test", "id");
        assertFalse(m.isTableEmpty());
        assertEquals(1, m.getMaxId());
        assertEquals(1, m.getMinId());
    }

    @Test
    public void ofForNotEmptyTable() throws Exception {
        insertData(100);
        StatTableInfo m = StatTableInfo.of(dataSource, "test", "id");
        assertFalse(m.isTableEmpty());
        assertEquals(100, m.getMaxId());
        assertEquals(1, m.getMinId());
    }
}