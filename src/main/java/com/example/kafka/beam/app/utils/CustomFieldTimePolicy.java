package com.example.kafka.beam.app.utils;

import com.example.kafka.beam.app.model.Tweet;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Optional;

public class CustomFieldTimePolicy extends TimestampPolicy<String, Tweet> {

    protected Instant currentWatermark;

    public CustomFieldTimePolicy(Optional<Instant> previousWatermark) {
        currentWatermark = previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }


    @Override
    public Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord<String, Tweet> record) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss X uuuu", Locale.ROOT);
        currentWatermark = new Instant(OffsetDateTime.parse(record.getKV().getValue().getCreatedAt(),
                dtf).toInstant().toEpochMilli());
        return currentWatermark;
    }

    @Override
    public Instant getWatermark(PartitionContext ctx) {
        return currentWatermark;
    }

}
