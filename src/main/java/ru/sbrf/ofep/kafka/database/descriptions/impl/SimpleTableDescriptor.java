package ru.sbrf.ofep.kafka.database.descriptions.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbrf.ofep.kafka.database.descriptions.TableDescriptor;
import ru.sbrf.ofep.kafka.database.dialects.SupportedVendor;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;


public class SimpleTableDescriptor implements TableDescriptor {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleTableDescriptor.class);

    private final SupportedVendor vendor;
    private final boolean tableExists;
    private final Map<String, FieldDescriptor> fields;

    private SimpleTableDescriptor(SupportedVendor vendor, boolean tableExists, Map<String, FieldDescriptor> fields) {
        this.vendor = vendor;
        this.tableExists = tableExists;
        this.fields = fields;
    }

    private static boolean doesTableExist(DatabaseMetaData dbMetaData, String catalog, String schema, String table) throws SQLException {
        try (ResultSet rs = dbMetaData.getTables(catalog, schema, toDBFormat(table), new String[]{"TABLE"})) {
            return rs.next();
        }
    }

    private static String getSchema(final Connection connection, final String product) throws SQLException {
        return SupportedVendor.of(product).getDBDetails().getSchema(connection);
    }


    public static SimpleTableDescriptor instanceFor(DataSource dataSource, String tableName) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            final DatabaseMetaData dbMetaData = connection.getMetaData();
            final String product = dbMetaData.getDatabaseProductName();
            final SupportedVendor vendor = SupportedVendor.of(product);
            final String catalog = connection.getCatalog();

            final String schema = getSchema(connection, product);
            final String tableNameForQuery = tableName.toUpperCase();

            final boolean doesTableExists = doesTableExist(dbMetaData, catalog, schema, tableNameForQuery);
            if(!doesTableExists) {
                return new SimpleTableDescriptor(vendor, doesTableExists, Collections.<String, FieldDescriptor>emptyMap());
            }

            return new SimpleTableDescriptor(vendor, doesTableExists, scanFields(dbMetaData, catalog, schema, tableName));
        }
    }

    private static String toDBFormat(String field) {
        return field.toUpperCase();
    }

    private static Map<String, FieldDescriptor> scanFields(DatabaseMetaData dbMetaData, String catalog, String schema, String table)
            throws SQLException {
        final Map<String, FieldDescriptor> result = new HashMap<>();
        setCommonFieldInfo(dbMetaData, catalog, schema, table, result);
        setPrimaryKeysInfo(dbMetaData, catalog, schema, table, result);
        return result;
    }

    private static void setPrimaryKeysInfo(DatabaseMetaData dbMetaData, String catalog, String schema,
                                           String table, Map<String, FieldDescriptor> result) throws SQLException {
        try (final ResultSet primaryKeysResultSet = dbMetaData.getPrimaryKeys(catalog, schema, toDBFormat(table))) {
            while (primaryKeysResultSet.next()) {
                final String fn = toDBFormat(primaryKeysResultSet.getString("COLUMN_NAME"));
                final FieldDescriptor fd = result.get(fn);
                if(fd != null) {
                    result.put(fn, FieldDescriptor.newInstance(fd.getType(), true, fd.isAutoIncrement()));
                }
            }
        }
    }

    private static void setCommonFieldInfo(DatabaseMetaData dbMetaData, String catalog, String schema,
                                           String table, Map<String, FieldDescriptor> result) throws SQLException {
        try (final ResultSet columnsResultSet = dbMetaData.getColumns(catalog, schema, toDBFormat(table), null)) {
            while (columnsResultSet.next()) {
                result.put(toDBFormat(columnsResultSet.getString("COLUMN_NAME")),
                        FieldDescriptor.newInstance(
                                columnsResultSet.getInt("DATA_TYPE"),
                                false,
                                columnsResultSet.getString("IS_AUTOINCREMENT").equalsIgnoreCase("yes")
                        ));
            }
        }
    }

    private FieldDescriptor getDescrByName(String s) {
        final FieldDescriptor d = fields.get(toDBFormat(s));
        if (d == null) {
            throw new IllegalArgumentException(format("Unknown field name %s", s));
        }
        return d;
    }

    @Override
    public SupportedVendor getVendor() {
        return vendor;
    }

    @Override
    public int getFieldType(String s) {
        return getDescrByName(s).getType();
    }

    @Override
    public boolean isTableExists() {
        return tableExists;
    }

    @Override
    public boolean isPrimaryKey(String s) {
        return getDescrByName(s).isPrimaryKey();
    }

    @Override
    public boolean isAutoIncrement(String s) {
        return getDescrByName(s).isAutoIncrement();
    }

    private static final class FieldDescriptor {
        private final boolean primaryKey;
        private final int type;
        private final boolean isAutoIncrement;


        FieldDescriptor(boolean primaryKey, int type, boolean isAutoIncrement) {
            this.primaryKey = primaryKey;
            this.type = type;
            this.isAutoIncrement = isAutoIncrement;
        }

        boolean isPrimaryKey() {
            return primaryKey;
        }

        int getType() {
            return type;
        }

        boolean isAutoIncrement() {
            return isAutoIncrement;
        }

        static FieldDescriptor newInstance(int type, boolean primaryKey, boolean isAutoIncrement) {
            return new FieldDescriptor(primaryKey, type, isAutoIncrement);
        }
    }
}
