package ru.sbrf.ofep.kafka.database.descriptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * Simple class. Describe some basic statistic information about table.
 *
 */
public class StatTableInfo {
    private static final Logger LOG = LoggerFactory.getLogger(StatTableInfo.class);
    private final long minId;
    private final long maxId;
    private final boolean tableEmpty;
    private final String tableName;

    private StatTableInfo(long minId, long maxId, String tableName) {
        this.minId = minId;
        this.maxId = maxId;
        this.tableEmpty = (minId == -1);
        this.tableName = tableName;
    }

    /**
     *
     * @param dataSource
     * @return null if could not query information caused any reason.
     */
    public static StatTableInfo of(DataSource dataSource, String tableName, String id) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return new StatTableInfo(
                    queryMinId(conn, tableName, id),
                    queryMaxId(conn, tableName, id),
                    tableName);
        }
    }

    private static long queryMinId(Connection conn, String table, String id) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement("select min(" + id + ") as min_index from " + table)) {
            return queryResultHelper(statement, "min_index");
        }
    }

    private static long queryMaxId(Connection conn, String table, String id) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement("select max(" + id + ") as max_index from " + table)) {
            return queryResultHelper(statement, "max_index");
        }
    }

    private static long queryResultHelper(PreparedStatement statement, String id) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next()) {
                long result = resultSet.getLong(id);
                return resultSet.wasNull() ? -1 : result;
            }
        }
        return -1;
    }

    public long getMinId() {
        return minId;
    }

    public long getMaxId() {
        return maxId;
    }

    public boolean isTableEmpty() {
        return tableEmpty;
    }

    @Override
    public String toString() {
        return "StatTableInfo{" +
                "minId=" + minId +
                ", maxId=" + maxId +
                ", tableEmpty=" + tableEmpty +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
