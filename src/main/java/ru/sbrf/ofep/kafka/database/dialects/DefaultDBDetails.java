package ru.sbrf.ofep.kafka.database.dialects;

import ru.sbrf.ofep.kafka.database.descriptions.impl.SimpleTableDescriptor;
import ru.sbrf.ofep.kafka.database.descriptions.impl.VendorSpecificDBDetails;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

class DefaultDBDetails implements VendorSpecificDBDetails {
    @Override
    public String getSchema(Connection connection) throws SQLException {
        return null;
    }

    @Override
    public List<String> getIdentityFields(Connection connection, String schema, String tableName) throws SQLException {
        return Collections.emptyList();
    }
}
