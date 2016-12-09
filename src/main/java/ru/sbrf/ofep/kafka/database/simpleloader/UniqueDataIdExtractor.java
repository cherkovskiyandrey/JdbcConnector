package ru.sbrf.ofep.kafka.database.simpleloader;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface UniqueDataIdExtractor {
    String extractAsString(ResultSet result) throws SQLException;
}


