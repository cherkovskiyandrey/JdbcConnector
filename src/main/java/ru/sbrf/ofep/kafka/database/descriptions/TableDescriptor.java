package ru.sbrf.ofep.kafka.database.descriptions;

import ru.sbrf.ofep.kafka.database.dialects.SupportedVendor;

public interface TableDescriptor {

    SupportedVendor getVendor();

    int getFieldType(String s);

    boolean isTableExists();

    boolean isPrimaryKey(String s);

    boolean isAutoIncrement(String s);

}
