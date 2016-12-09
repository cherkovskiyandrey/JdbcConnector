package ru.sbrf.ofep.kafka;

import org.apache.kafka.connect.source.SourceRecord;
import ru.sbrf.ofep.kafka.database.descriptions.StatTableInfo;

import java.util.List;

public interface DataLoader {

    List<SourceRecord> loadData(StatTableInfo info);

    void interrupt();
}
