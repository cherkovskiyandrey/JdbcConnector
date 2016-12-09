package ru.sbrf.ofep.kafka.database.descriptions.impl;

import java.sql.Connection;
import java.sql.SQLException;

public interface VendorSpecificDBDetails {

    String getSchema(final Connection connection) throws SQLException;
}
