package ru.sbrf.ofep.kafka.config;

import org.apache.kafka.common.config.AbstractConfig;

import static ru.sbrf.ofep.kafka.config.ConfigParam.*;

public class DbConfig {
    private final String topic;
    private final String dbUrl;
    private final String dbLogin;
    private final String dbPswd;
    private final String tableName;
    private final String tableId;
    private final String tableData;
    private final long batchSize;
    private final long pollInterval;
    private final String uniqueDataId;
    private final String dataTimestamp;


    public DbConfig(String topic,
                    String dbUrl,
                    String dbLogin,
                    String dbPswd,
                    String tableName,
                    String tableId,
                    String tableData,
                    long batchSize,
                    long pollInterval,
                    String uniqueDataId,
                    String dataTimestamp) {
        this.topic = topic;
        this.dbUrl = dbUrl;
        this.dbLogin = dbLogin;
        this.dbPswd = dbPswd;
        this.tableName = tableName;
        this.tableId = tableId;
        this.tableData = tableData;
        this.batchSize = batchSize;
        this.pollInterval = pollInterval;
        this.uniqueDataId = uniqueDataId;
        this.dataTimestamp = dataTimestamp;
    }

    public static DbConfig of(AbstractConfig configuration) {
        return new DbConfig(
                configuration.getString(TOPIC_CONFIG.getName()),
                configuration.getString(DATA_BASE_URL.getName()),
                configuration.getString(DATA_BASE_LOGIN.getName()),
                configuration.getString(DATA_BASE_PSWD.getName()),
                configuration.getString(DATA_BASE_TABLE.getName()),
                configuration.getString(DATA_BASE_TABLE_ID.getName()),
                configuration.getString(DATA_BASE_TABLE_CLOB.getName()),
                configuration.getLong(DATA_BASE_TABLE_BATCH_SIZE.getName()),
                configuration.getInt(DATA_BASE_POLL_INTERVAL.getName()),
                configuration.getString(DATA_BASE_TABLE_DATA_UNIQUE_ID.getName()),
                configuration.getString(DATA_BASE_TABLE_DATA_TIMESTAMP.getName())
        );
    }

    public String getTopic() {
        return topic;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public String getDbLogin() {
        return dbLogin;
    }

    public String getDbPswd() {
        return dbPswd;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableId() {
        return tableId;
    }

    public String getTableData() {
        return tableData;
    }

    public long getBatchSize() {
        return batchSize;
    }

    public long getPollInterval() {
        return pollInterval;
    }

    public String getUniqueDataId() {
        return uniqueDataId;
    }

    public String getDataTimestamp() {
        return dataTimestamp;
    }
}
