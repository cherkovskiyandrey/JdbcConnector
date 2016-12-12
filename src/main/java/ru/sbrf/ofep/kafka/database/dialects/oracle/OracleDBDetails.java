package ru.sbrf.ofep.kafka.database.dialects.oracle;

import ru.sbrf.ofep.kafka.database.descriptions.impl.VendorSpecificDBDetails;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

import static ru.sbrf.ofep.kafka.Utils.toDBFormat;

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

    @Override
    public List<String> getIdentityFields(Connection connection, String schema, String tableName) throws SQLException {
        final List<String> result = new LinkedList<>();
        try(final PreparedStatement preparedStatement = connection.
                prepareStatement("select COLUMN_NAME from all_tab_identity_cols where OWNER = ? and TABLE_NAME = ?")) {

            preparedStatement.setString(1, schema);
            preparedStatement.setString(2, toDBFormat(tableName));

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(resultSet.getString("COLUMN_NAME"));
                }
            }
        }
        return result;
    }
}
