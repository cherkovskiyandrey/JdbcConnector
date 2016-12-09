package ru.sbrf.ofep.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbrf.ofep.kafka.config.ConfigParam;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcConnector extends SourceConnector {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcConnector.class);

    public static final String VERSION = AppInfoParser.getVersion();
    public static final ConfigDef CONFIG_DEF = ConfigParam.defineAll(new ConfigDef());
    public static final String OFFSET_ID_NAME = "OFFSET";

    private final Map<String, String> config = new HashMap<>();

    @Override
    public String version() {
        return VERSION; //TODO
    }

    @Override
    public void start(Map<String, String> props) {
        LOG.info("Starting connector with properties: " + props);
        config.putAll(props);
    }

    @Override
    public Class<? extends JdbcTask> taskClass() {
        return JdbcTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Arrays.asList(config);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
