package ru.sbrf.ofep.kafka;


import com.google.gson.annotations.SerializedName;

import java.util.Objects;

//TODO: потом перетащить в общую структуру данных
public class KafkaKey {
    @SerializedName("UUID")
    private String uniqueRecordId;

    @SerializedName("TIMESTAMP")
    private String timestamp;

    public KafkaKey() {
    }

    private KafkaKey(String uniqueRecordId, String timestamp) {
        this.uniqueRecordId = uniqueRecordId;
        this.timestamp = timestamp;
    }

    public static KafkaKey of(String uniqueRecordId, String timestamp) {
        return new KafkaKey(uniqueRecordId, timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaKey kafkaKey = (KafkaKey) o;
        return Objects.equals(uniqueRecordId, kafkaKey.uniqueRecordId) &&
                Objects.equals(timestamp, kafkaKey.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uniqueRecordId, timestamp);
    }

    @Override
    public String toString() {
        return "KafkaKey{" +
                "uniqueRecordId='" + uniqueRecordId + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
