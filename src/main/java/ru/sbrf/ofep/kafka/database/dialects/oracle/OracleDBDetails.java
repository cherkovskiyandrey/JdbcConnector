package ru.sbrf.ofep.kafka.database.dialects.oracle;

import ru.sbrf.ofep.kafka.database.descriptions.impl.VendorSpecificDBDetails;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OracleDBDetails implements VendorSpecificDBDetails {
    @Override
    public String getSchema(Connection connection) throws SQLException {
        // Use SQL to retrieve the database name for Oracle, apparently the JDBC API doesn't work as expected
        try (
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("select sys_context('userenv','current_schema') x from dual")
        ) {
            if (rs.next()) {
                return rs.getString("x").toUpperCase();
            } else {
                throw new SQLException("Failed to determine Oracle schema");
            }
        }
    }
}
