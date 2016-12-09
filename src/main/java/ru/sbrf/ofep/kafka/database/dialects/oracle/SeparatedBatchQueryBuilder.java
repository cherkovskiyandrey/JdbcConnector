package ru.sbrf.ofep.kafka.database.dialects.oracle;

import ru.sbrf.ofep.kafka.database.simpleloader.SeparatedBatchReadStream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SeparatedBatchQueryBuilder implements SeparatedBatchReadStream.QueryBuilder {

    //According to http://www.oracle.com/technetwork/issue-archive/2006/06-sep/o56asktom-086197.html
    @Override
    public PreparedStatement buildQuery(Connection conn, String tableName, String tableId, long from, long batchSize) throws SQLException {
        final String queryString = "select * from (select * from " +
                tableName +
                " where " +
                tableId +
                " >= ? order by " +
                tableId +
                " asc) where ROWNUM <= ?";

        final PreparedStatement st = conn.prepareStatement(queryString, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        st.setFetchSize((int) batchSize);
        st.setLong(1, from);
        st.setLong(2, batchSize);

        return st;
    }
}
