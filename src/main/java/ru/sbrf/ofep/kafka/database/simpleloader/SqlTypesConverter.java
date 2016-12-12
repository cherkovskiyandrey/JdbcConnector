package ru.sbrf.ofep.kafka.database.simpleloader;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;


enum SqlTypesConverter {
    //Strings
    CHAR(Types.CHAR) {
        @Override
        byte[] extractAsByteArray(ResultSet result, String dataColumnName) throws SQLException {
            String s = result.getString(dataColumnName);
            if (s == null) {
                return EMPTY_ARRAY;
            }
            return s.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        String extractAsString(ResultSet result, String dataColumnName) throws SQLException {
            return result.getString(dataColumnName);
        }
    },
    VARCHAR(Types.VARCHAR) {
        @Override
        byte[] extractAsByteArray(ResultSet result, String dataColumnName) throws SQLException {
            return CHAR.extractAsByteArray(result, dataColumnName);
        }

        @Override
        String extractAsString(ResultSet result, String dataColumnName) throws SQLException {
            return result.getString(dataColumnName);
        }
    },
    LONGVARCHAR(Types.LONGVARCHAR) {
        @Override
        byte[] extractAsByteArray(ResultSet result, String dataColumnName) throws SQLException {
            return CHAR.extractAsByteArray(result, dataColumnName);
        }

        @Override
        String extractAsString(ResultSet result, String dataColumnName) throws SQLException {
            return result.getString(dataColumnName);
        }
    },
    CLOB(Types.CLOB) {
        @Override
        byte[] extractAsByteArray(ResultSet result, String dataColumnName) throws SQLException {
            final Clob r = result.getClob(dataColumnName);
            if (r == null) {
                return EMPTY_ARRAY;
            }
            try {
                return IOUtils.toByteArray(r.getCharacterStream(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }

        @Override
        String extractAsString(ResultSet result, String dataColumnName) throws SQLException {
            return new String(extractAsByteArray(result, dataColumnName), StandardCharsets.UTF_8);
        }
    },

    // Numbers
    BIGINT(Types.BIGINT) {
        @Override
        byte[] extractAsByteArray(ResultSet result, String dataColumnName) throws SQLException {
            return CHAR.extractAsByteArray(result, dataColumnName);
        }

        @Override
        String extractAsString(ResultSet result, String dataColumnName) throws SQLException {
            return result.getString(dataColumnName);
        }
    },
    SMALLINT(Types.SMALLINT) {
        @Override
        byte[] extractAsByteArray(ResultSet result, String dataColumnName) throws SQLException {
            return CHAR.extractAsByteArray(result, dataColumnName);
        }

        @Override
        String extractAsString(ResultSet result, String dataColumnName) throws SQLException {
            return result.getString(dataColumnName);
        }
    },
    INTEGER(Types.INTEGER) {
        @Override
        byte[] extractAsByteArray(ResultSet result, String dataColumnName) throws SQLException {
            return CHAR.extractAsByteArray(result, dataColumnName);
        }

        @Override
        String extractAsString(ResultSet result, String dataColumnName) throws SQLException {
            return result.getString(dataColumnName);
        }
    },

    // Timestamps
    DATE(Types.DATE) {
        @Override
        byte[] extractAsByteArray(ResultSet result, String dataColumnName) throws SQLException {
            Date d = result.getDate(dataColumnName);
            if (d == null) {
                return EMPTY_ARRAY;
            }
            return dateFormat.get().format(d).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        String extractAsString(ResultSet result, String dataColumnName) throws SQLException {
            Date d = result.getDate(dataColumnName);
            if (d == null) {
                return null;
            }
            return dateFormat.get().format(d);
        }
    },
    TIMESTAMP(Types.TIMESTAMP) {
        @Override
        byte[] extractAsByteArray(ResultSet result, String dataColumnName) throws SQLException {
            Timestamp d = result.getTimestamp(dataColumnName);
            if (d == null) {
                return EMPTY_ARRAY;
            }
            return dateFormat.get().format(d).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        String extractAsString(ResultSet result, String dataColumnName) throws SQLException {
            Timestamp d = result.getTimestamp(dataColumnName);
            if (d == null) {
                return null;
            }
            return dateFormat.get().format(d);
        }
    },
    TIMESTAMPTZ(2014, 1111, -101) {
        @Override
        byte[] extractAsByteArray(ResultSet result, String dataColumnName) throws SQLException {
            return TIMESTAMP.extractAsByteArray(result, dataColumnName);
        }

        @Override
        String extractAsString(ResultSet result, String dataColumnName) throws SQLException {
            return TIMESTAMP.extractAsString(result, dataColumnName);
        }
    },
    ;

    private final static ThreadLocal<SimpleDateFormat> dateFormat = new ThreadLocal<SimpleDateFormat>(){
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        }
    };
    private final static byte[] EMPTY_ARRAY = new byte[0];
    private final int[] jdbcType;

    SqlTypesConverter(int... jdbcType) {
        this.jdbcType = jdbcType;
    }

    static SqlTypesConverter of(int jdbcType) {
        for (SqlTypesConverter conv : values()) {
            for(int type : conv.jdbcType) {
                if (type == jdbcType) {
                    return conv;
                }
            }
        }
        return null;
    }

    abstract byte[] extractAsByteArray(ResultSet result, String dataColumnName) throws SQLException;

    abstract String extractAsString(ResultSet result, String dataColumnName) throws SQLException;
}
