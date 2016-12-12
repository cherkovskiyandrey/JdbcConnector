package ru.sbrf.ofep.kafka.database.dialects;

import ru.sbrf.ofep.kafka.database.descriptions.impl.VendorSpecificDBDetails;
import ru.sbrf.ofep.kafka.database.dialects.oracle.OracleDBDetails;
import ru.sbrf.ofep.kafka.database.dialects.oracle.SeparatedBatchQueryBuilder;
import ru.sbrf.ofep.kafka.database.simpleloader.SeparatedBatchReadStream;

import java.sql.Connection;
import java.sql.SQLException;

public enum SupportedVendor {
    ORACLE("oracle", new SeparatedBatchQueryBuilder(), new OracleDBDetails()),

    H2("h2", new SeparatedBatchQueryBuilder(), new DefaultDBDetails())
    ;

    private final String vendor;
    private final VendorSpecificDBDetails vendorSpecificDBDetails;
    private final SeparatedBatchReadStream.QueryBuilder queryBuilder;

    SupportedVendor(String vendor,
                    SeparatedBatchReadStream.QueryBuilder queryBuilder,
                    VendorSpecificDBDetails vendorSpecificDBDetails) {
        this.vendor = vendor;
        this.vendorSpecificDBDetails = vendorSpecificDBDetails;
        this.queryBuilder = queryBuilder;
    }

    public VendorSpecificDBDetails getDBDetails() {
        return vendorSpecificDBDetails;
    }

    public SeparatedBatchReadStream.QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public static SupportedVendor of(String product) {
        for(SupportedVendor supportedVendor : values()) {
            if(supportedVendor.vendor.equalsIgnoreCase(product)) {
                return supportedVendor;
            }
        }
        throw new IllegalArgumentException(product);
    }
}
