package ru.sbrf.ofep.kafka.database.simpleloader;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;


enum AnyTypeExtractor {
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
    },;

    private final static byte[] EMPTY_ARRAY = new byte[0];
    private final int jdbcType;

    AnyTypeExtractor(int jdbcType) {
        this.jdbcType = jdbcType;
    }

    static AnyTypeExtractor of(int jdbcType) {
        for (AnyTypeExtractor t : values()) {
            if (t.getJdbcType() == jdbcType) {
                return t;
            }
        }
        return null;
    }

    int getJdbcType() {
        return jdbcType;
    }

    abstract byte[] extractAsByteArray(ResultSet result, String dataColumnName) throws SQLException;

    abstract String extractAsString(ResultSet result, String dataColumnName) throws SQLException;
}
