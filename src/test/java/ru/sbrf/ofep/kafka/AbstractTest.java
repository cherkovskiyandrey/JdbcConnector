package ru.sbrf.ofep.kafka;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.After;
import org.junit.Before;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class AbstractTest {
    protected DataSource dataSource;

    @Before
    public void init() throws ClassNotFoundException, SQLException {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:h2:mem:dbTest;MODE=Oracle;DB_CLOSE_DELAY=-1");
        dataSource.setUsername("sa");
        dataSource.setPassword("");
        dataSource.setMaximumPoolSize(1);
        this.dataSource = dataSource;

        try (Connection conn = dataSource.getConnection();
             Statement st = conn.createStatement()) {
            st.execute("create table test(" +
                    "id INTEGER PRIMARY KEY auto_increment, " +
                    "uuid VARCHAR2(100 CHAR), " +
                    "data VARCHAR2(100 CHAR), " +
                    "clob_data CLOB NULL, " +
                    ")");
        }
    }

    @After
    public void teardown() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement st = conn.createStatement()) {
            st.execute("drop table test");
        }
        ((HikariDataSource) dataSource).close();
    }
}
