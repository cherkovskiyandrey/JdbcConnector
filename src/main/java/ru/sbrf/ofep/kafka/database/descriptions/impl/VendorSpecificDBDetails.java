package ru.sbrf.ofep.kafka.database.descriptions.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public interface VendorSpecificDBDetails {

    String getSchema(final Connection connection) throws SQLException;

    List<String> getIdentityFields(Connection connection, String schema, String tableName) throws SQLException;
}
