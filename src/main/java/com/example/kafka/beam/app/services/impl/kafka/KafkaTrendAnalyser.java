package com.example.kafka.beam.app.services.impl.kafka;

import com.example.kafka.beam.app.model.Tweet;
import com.example.kafka.beam.app.services.dofns.PrintKafkaRecord;
import com.example.kafka.beam.app.utils.KafkaConfig;
import com.example.kafka.beam.app.serializer.TweetDeserializer;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;

@Slf4j
@Service
public class KafkaTrendAnalyser {

    public void getTrendComparisonWithWindowing() {
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
            .withTopic("news")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(TweetDeserializer.class)
                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest")));
//                .withoutMetadata()
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
        lines.apply(MapElements.via(new PrintKafkaRecord()));
//            .apply("Convert Tweet to String", MapElements.into(TypeDescriptors.strings()).via(Object::toString))
//            .apply(TextIO.write().to("output.txt").withNumShards(1));

        pipeline.run().waitUntilFinish();
        return;
    }

}
