package ru.sbrf.ofep.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sbrf.ofep.kafka.database.Cursor;
import ru.sbrf.ofep.kafka.database.DataBaseClient;
import ru.sbrf.ofep.kafka.database.DataBaseReadStream;
import ru.sbrf.ofep.kafka.database.simpleloader.SeparatedBatchClient;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static ru.sbrf.ofep.kafka.JdbcConnector.CONFIG_DEF;
import static ru.sbrf.ofep.kafka.JdbcConnector.VERSION;
import static ru.sbrf.ofep.kafka.config.ConfigParam.DATA_BASE_POLL_INTERVAL;

public class JdbcTask extends SourceTask {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcTask.class);

    private final CountDownLatch stopSignal = new CountDownLatch(1);
    private volatile boolean stopFlag = false;
    private AbstractConfig configuration;
    private DataBaseClient dataBaseClient;
    private DataBaseReadStream dataBaseReadStream;
    private Cursor currentCursor;

    @Override
    public String version() {
        return VERSION;
    }

    //For test
    public void start(Map<String, String> props, DataBaseClient dataBaseClient, Cursor currentCursor) {
        LOG.info("Starting task with properties: " + props);

        this.configuration = new AbstractConfig(CONFIG_DEF, props);
        this.dataBaseClient = dataBaseClient;
        this.currentCursor = currentCursor;
    }

    @Override
    public void start(Map<String, String> props) {
        LOG.info("Starting task with properties: " + props);

        this.configuration = new AbstractConfig(CONFIG_DEF, props);
        this.dataBaseClient = SeparatedBatchClient.newInstance(configuration);
        this.currentCursor = dataBaseClient.newCursorFromOffsetStorage(context.offsetStorageReader()).next();
    }


    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        LOG.trace("Begin poll.");
        if (stopFlag) {
            LOG.info("Stop flag has been detected.");
            return Collections.emptyList();
        }
        List<SourceRecord> result = Collections.emptyList();
        try {
            recreateIfNeed();
            result = dataBaseReadStream.read();
            LOG.trace("Size of records which has been read: " + result.size());
        } catch (SQLException e) { // Suppose that mostly connection lost exception. Otherwise recreateIfNeed() check prerequisites and throw IllegalStateError on the next step.
            handleError(e);
        }
        if (result.isEmpty()) {
            sleep();
        }
        return result;
    }

    private void handleError(SQLException e) {
        LOG.warn("Potential recoverable error has been accoutered: ", e);
        updateCurrentCursorIfNeed();
        destroyReader();
    }

    private void destroyReader() {
        Utils.closeQuietly(dataBaseReadStream);
        dataBaseReadStream = null;
        LOG.trace("Read stream has been closed.");
    }

    private void updateCurrentCursorIfNeed() {
        if (!isReaderExists()) {
            currentCursor = dataBaseReadStream.currentCursor();
            LOG.trace("Remember current cursor from breaking stream: " + currentCursor);
        }
    }

    private void recreateIfNeed() throws SQLException {
        if (!isReaderExists()) {
            LOG.trace("Try to create new stream.");
            dataBaseReadStream = dataBaseClient.createNewStream(currentCursor);
        }
    }

    private void sleep() throws InterruptedException {
        stopSignal.await(configuration.getInt(DATA_BASE_POLL_INTERVAL.getName()), TimeUnit.SECONDS);
    }

    @Override
    public synchronized void stop() {
        LOG.info("Stopping task...");
        if (stopFlag) {
            LOG.warn("Signal 'stop' has been already sent!");
        }
        stopFlag = true;
        stopSignal.countDown();
        Utils.closeQuietly(dataBaseReadStream);
        Utils.closeQuietly(dataBaseClient);
        LOG.info("Signal 'stop' has been sent.");
    }

    private boolean isReaderExists() {
        return dataBaseReadStream != null;
    }
}
