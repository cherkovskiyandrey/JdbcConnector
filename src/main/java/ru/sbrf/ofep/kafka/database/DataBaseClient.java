package ru.sbrf.ofep.kafka.database;

import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.sql.SQLException;

public interface DataBaseClient extends AutoCloseable {

    /**
     * Create new stream for reading from data base.
     *
     * @param beginCursor begin cursor
     * @return
     */
    DataBaseReadStream createNewStream(Cursor beginCursor) throws SQLException;

    /**
     * Create new cursor from offset storage.
     *
     * @param offsetStorageReader
     * @return {@link Cursor#UNDEFINED} if data can not be obtained from offset reader.
     */
    Cursor newCursorFromOffsetStorage(OffsetStorageReader offsetStorageReader);
}
