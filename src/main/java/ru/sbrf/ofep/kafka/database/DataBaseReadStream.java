package ru.sbrf.ofep.kafka.database;

import org.apache.kafka.connect.source.SourceRecord;

import java.sql.SQLException;
import java.util.List;

public interface DataBaseReadStream extends AutoCloseable {

    /**
     * Read next portion of data from.
     *
     * @return
     * @throws SQLException
     */
    List<SourceRecord> read() throws SQLException;


    /**
     * Return current cursor.
     *
     * @return
     */
    Cursor currentCursor();
}
