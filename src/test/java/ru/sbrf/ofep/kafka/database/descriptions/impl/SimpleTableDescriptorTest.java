package ru.sbrf.ofep.kafka.database.descriptions.impl;

import org.junit.Test;
import ru.sbrf.ofep.kafka.AbstractTest;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Types;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimpleTableDescriptorTest extends AbstractTest {

    @Test(expected = SQLException.class)
    public void brokenConnection() throws SQLException {
        final DataSource mockDataSource = mock(DataSource.class);
        when(mockDataSource.getConnection()).thenThrow(new SQLException("Connection lost"));

        SimpleTableDescriptor.instanceFor(mockDataSource, "does_not_matter");
    }

    @Test
    public void notExistsTable() throws SQLException {
        SimpleTableDescriptor descriptor = SimpleTableDescriptor.instanceFor(dataSource, "not_exists_table");
        assertFalse(descriptor.isTableExists());
    }

    @Test
    public void existsTable() throws SQLException {
        SimpleTableDescriptor descriptor = SimpleTableDescriptor.instanceFor(dataSource, "test");
        assertTrue(descriptor.isTableExists());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPrimaryKeyForUnknownField() throws SQLException {
        SimpleTableDescriptor descriptor = SimpleTableDescriptor.instanceFor(dataSource, "test");
        descriptor.isPrimaryKey("unknown_field");
    }

    @Test
    public void testPrimaryKey() throws SQLException {
        SimpleTableDescriptor descriptor = SimpleTableDescriptor.instanceFor(dataSource, "test");
        assertTrue(descriptor.isPrimaryKey("id"));
        assertFalse(descriptor.isPrimaryKey("data"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFieldTypeForUnknownField() throws SQLException {
        SimpleTableDescriptor descriptor = SimpleTableDescriptor.instanceFor(dataSource, "test");
        descriptor.getFieldType("unknown_field");
    }

    @Test
    public void testFieldType() throws SQLException {
        SimpleTableDescriptor descriptor = SimpleTableDescriptor.instanceFor(dataSource, "test");
        assertEquals(Types.INTEGER, descriptor.getFieldType("id"));
        assertEquals(Types.VARCHAR, descriptor.getFieldType("data"));
    }

    @Test
    public void testAutoIncremetn() throws SQLException {
        SimpleTableDescriptor descriptor = SimpleTableDescriptor.instanceFor(dataSource, "test");
        assertTrue(descriptor.isAutoIncrement("id"));
        assertFalse(descriptor.isAutoIncrement("data"));
    }
}