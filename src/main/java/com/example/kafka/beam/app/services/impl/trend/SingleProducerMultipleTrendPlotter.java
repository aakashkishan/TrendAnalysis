package com.example.kafka.beam.app.services.impl.trend;

import com.example.kafka.beam.app.model.Tweet;
import com.example.kafka.beam.app.serializer.TweetDeserializer;
import com.example.kafka.beam.app.services.dofns.CheckMultipleTrendSingleProducer;
import com.example.kafka.beam.app.services.dofns.PrintKV;
import com.example.kafka.beam.app.utils.CustomFieldTimePolicy;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;

@Service
public class SingleProducerMultipleTrendPlotter {

    public void plotSingleProducerMultipleTwitterTrend(String[] trends) {
        final String twitterFormat="EEE MMM dd HH:mm:ss ZZZZZ yyyy";
        SimpleDateFormat sf = new SimpleDateFormat(twitterFormat);

        SingleProducerMultipleTrentPlotterPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs("")
                .withValidation().as(SingleProducerMultipleTrentPlotterPipelineOptions.class);
        pipelineOptions.setRunner(FlinkRunner.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        // Read tweets from the JSONL File
        pipeline.apply(KafkaIO.<String, Tweet>read().withBootstrapServers(pipelineOptions.getBootstrapServer())
                .withTopic(pipelineOptions.getKafkaTopic())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(TweetDeserializer.class)
                .withTimestampPolicyFactory((tp, previousWaterMark) -> new CustomFieldTimePolicy(previousWaterMark))
                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"))
                .withoutMetadata())
                .apply("GetTheTweets", Values.<Tweet>create())
                .apply("SetTimeStamps", WithTimestamps.of(
                        (Tweet tweet) -> {
                            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss X uuuu", Locale.ROOT);
                            return new Instant(OffsetDateTime.parse(tweet.getCreatedAt(), dtf).toInstant().toEpochMilli());
                        }
                ))
                .apply("FixedWindowsOfOneDay", Window.into(FixedWindows.of(Duration.standardDays(1))))
                .apply("CheckTrend", ParDo.of(new CheckMultipleTrendSingleProducer(Arrays.asList(trends))))
                .apply("CountPerElement", Count.perElement())
                .apply("ConvertCountIntoOutputString", ParDo.of(new PrintKV(true)))
                .apply(TextIO.write().to(pipelineOptions.getOutputFile()).withWindowedWrites().withNumShards(1));
        System.out.println("Check Trends with Tumbling Windows");

        pipeline.run().waitUntilFinish();

        return;
    }

}
