package ru.sbrf.ofep.kafka.config;

import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public enum ConfigParam {
    TOPIC_CONFIG("topic", ConfigDef.Type.STRING, null, NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Output topic."),

    DATA_BASE_URL("db.url", ConfigDef.Type.STRING, null, NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Data base url."),

    DATA_BASE_LOGIN("db.login", ConfigDef.Type.STRING, null, NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Data base login."),

    DATA_BASE_PSWD("db.pswd", ConfigDef.Type.STRING, null, NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Data base password."),

    DATA_BASE_TABLE("db.table.name", ConfigDef.Type.STRING, null, NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Data base table."),

    DATA_BASE_TABLE_ID("db.table.id", ConfigDef.Type.STRING, null, NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Data base primary number format key."),

    DATA_BASE_TABLE_CLOB("db.table.data", ConfigDef.Type.STRING, null, NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
            "Data base target data field name (string, clob)."),

    DATA_BASE_TABLE_BATCH_SIZE("db.batch.size", ConfigDef.Type.LONG, ConfigDef.Range.atLeast(1000), 1000, ConfigDef.Importance.LOW,
            "Data base batch size."),

    DATA_BASE_POLL_INTERVAL("db.poll.interval", ConfigDef.Type.INT, ConfigDef.Range.atLeast(1), 10, ConfigDef.Importance.LOW,
            "Data base poll interval in seconds."),

    DATA_BASE_TABLE_DATA_UNIQUE_ID("db.table.data.unique.id", ConfigDef.Type.STRING, null, null, ConfigDef.Importance.LOW,
            "Unique target data id. If present used as kafka key. If not present used table.id field"),

    DATA_BASE_TABLE_DATA_TIMESTAMP("db.table.data.timestamp", ConfigDef.Type.STRING, null, null, ConfigDef.Importance.LOW,
            "Field with timestamp or record. If present used as part of kafka key."),
    ;

    private final String name;
    private final ConfigDef.Type type;
    private final ConfigDef.Importance importance;
    private final String descriptor;
    private final ConfigDef.Validator validator;
    private final Object defaultValue;

    ConfigParam(String name, ConfigDef.Type type, ConfigDef.Validator validator,
                Object defaultValue, ConfigDef.Importance importance, String descriptor) {
        this.name = name;
        this.type = type;
        this.validator = validator;
        this.defaultValue = defaultValue;
        this.importance = importance;
        this.descriptor = descriptor;
    }

    public static ConfigDef defineAll(ConfigDef orig) {
        ConfigDef result = orig;
        for (ConfigParam c : values()) {
            result = c.define(result);
        }
        return result;
    }

    public String getName() {
        return name;
    }

    public ConfigDef.Type getType() {
        return type;
    }

    public ConfigDef.Importance getImportance() {
        return importance;
    }

    public String getDescriptor() {
        return descriptor;
    }

    public ConfigDef define(ConfigDef orig) {
        return orig.define(name, type, defaultValue, validator, importance, descriptor);
    }
}
