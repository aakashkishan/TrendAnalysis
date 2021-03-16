package com.example.kafka.beam.app.services.impl.kafka;

import com.example.kafka.beam.app.model.Tweet;
import com.example.kafka.beam.app.utils.KafkaConfig;
import com.example.kafka.beam.app.serializer.TweetDeserializer;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.List;

@Slf4j
@Service
public class KafkaTrendAnalyser {

    public void getTrendComparisonWithWindowing(String[] trends, List<String> topics) {
        Duration WINDOW_TIME = Duration.standardDays(1);
        Duration ALLOWED_LATENESS = Duration.standardHours(6);
        final String twitterFormat="EEE MMM dd HH:mm:ss ZZZZZ yyyy";
        SimpleDateFormat sf = new SimpleDateFormat(twitterFormat);

        final int[] counter = {1};

        // Initialize the pipeline
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        pipelineOptions.setRunner(FlinkRunner.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<KafkaRecord<String, Tweet>> lines = pipeline.apply(KafkaIO.<String, Tweet>read().withBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
            .withTopic("cnbc")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(TweetDeserializer.class)
                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"))
                .withMaxNumRecords(10));
//                .withoutMetadata())
//                .apply("Get the Values", Values.<Tweet>create());
//                .apply("TimeStamp the Tweets", WithTimestamps.of(
//                    (Tweet t) -> {
//                        try {
//                            return Instant.ofEpochMilli(sf.parse(t.getCreatedAt()).getTime());
//                        } catch (ParseException e) {
//                            e.printStackTrace();
//                        }
//                        return null;
//                    }))
//                .apply("Apply Window", Window.<Tweet>into(FixedWindows.of(WINDOW_TIME))
//                    .withAllowedLateness(ALLOWED_LATENESS)
//                    .triggering(AfterWatermark.pastEndOfWindow())
//                    .accumulatingFiredPanes())
//                .apply("Check For Trend", ParDo.of(new DoFn<Tweet, String>() {
//                    @ProcessElement
//                    public void processElement(ProcessContext c) {
//                        String text = c.element().getFullText();
//                        for(String trend: trends) {
//                            if(text.contains(trend)) {
//                                c.output(trend);
//                            }
//                        }
//                    }
//                }))
//                .apply(Count.perElement()).apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
//                    @Override
//                    public String apply(KV<String, Long> input) {
//                        System.out.println(String.format("%s => %s", input.getKey(), input.getValue()));
//                        return String.format("%s => %s", input.getKey(), input.getValue());
//                    }
//                }))
        lines.apply(MapElements.via(new SimpleFunction<KafkaRecord<String, Tweet>, Tweet>() {
            @Override
            public Tweet apply(KafkaRecord<String, Tweet> kafkaRecord) {
                return kafkaRecord.getKV().getValue();
            }
        }))
            .apply("Convert Tweet to String", MapElements.into(TypeDescriptors.strings()).via(Object::toString))
            .apply(TextIO.write().to("output.txt").withNumShards(1));

        pipeline.run().waitUntilFinish();
        return;
    }

    public void getTrendComparisonWithSplittableWindowing(String[] trends, List<String> topics) {
        Duration WINDOW_TIME = Duration.standardDays(1);
        Duration ALLOWED_LATENESS = Duration.standardHours(6);
        Long OFFSET_LENGTH = new Long(1000);
        final String twitterFormat="EEE MMM dd HH:mm:ss ZZZZZ yyyy";
        SimpleDateFormat sf = new SimpleDateFormat(twitterFormat);

        // Initialize the pipeline
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        pipelineOptions.setRunner(FlinkRunner.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        pipeline.apply(KafkaIO.<String, String>read().withBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                .withTopic("cnbc")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"))
                .withMaxNumRecords(10)
                .withoutMetadata())
                .apply("Get the Values", Values.<String>create())
//                .apply("TimeStamp the Tweets", WithTimestamps.of(
//                    (Tweet t) -> Instant.ofEpochMilli(t.getCreatedAt().getTime())))
//                .apply("Apply Window", Window.<Tweet>into(FixedWindows.of(WINDOW_TIME))
//                        .withAllowedLateness(ALLOWED_LATENESS)
//                        .triggering(AfterWatermark.pastEndOfWindow())
//                        .accumulatingFiredPanes())
//                .apply("Check For Trend", ParDo.of(new DoFn<Tweet, String>() {
//                    @ProcessElement
//                    public void processElement(ProcessContext c, OffsetRangeTracker tracker) {
////                        consumer.seek(tracker.start());
//                        String text = c.element().getFullText();
//                        for(String trend: trends) {
//                            if(text.contains(trend)) {
////                                if(tracker.tryClaim(record.offset()))
//                                c.output(trend);
//                            }
//                        }
//                    }
//
//                    @GetInitialRestriction
//                    OffsetRange getInitialRestriction(String topic) {
//                        return new OffsetRange(0, OFFSET_LENGTH);
//                    }
//
//                    @NewTracker
//                    OffsetRangeTracker newTracker(OffsetRange range) {
//                        return new OffsetRangeTracker(OFFSET_LENGTH, OffsetRangeTracker.OFFSET_INFINITY);
//                    }
//                }))
//                .apply(Count.perElement()).apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
//            @Override
//            public String apply(KV<String, Long> input) {
//                System.out.println(String.format("%s => %s", input.getKey(), input.getValue()));
//                return String.format("%s => %s", input.getKey(), input.getValue());
//            }
//        }))
            .apply(MapElements.into(TypeDescriptors.strings()).via(Object::toString))
            .apply(TextIO.write().to("output.txt"));

        pipeline.run().waitUntilFinish();
        return;
    }

}
