package com.example.kafka.beam.app.services.impl.text;

import com.example.kafka.beam.app.model.Tweet;
import com.example.kafka.beam.app.services.dofns.CheckMultipleTrendSingleProducer;
import com.example.kafka.beam.app.services.dofns.PrintKV;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Arrays;

@Service
public class SingleProducerMultipleTrendPlotter {

    public void plotSingleTwitterTrend(String[] trends) {
        final String twitterFormat="EEE MMM dd HH:mm:ss ZZZZZ yyyy";
        SimpleDateFormat sf = new SimpleDateFormat(twitterFormat);

        SingleProducerMultipleTrentPlotterPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs("")
                .withValidation().as(SingleProducerMultipleTrentPlotterPipelineOptions.class);
        pipelineOptions.setRunner(FlinkRunner.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        // Read tweets from the JSONL File
        PCollection<String> tweetLines = pipeline.apply("ReadLines", TextIO.read()
                .from(pipelineOptions.getInputFile()));
//                .watchForNewFiles(Duration.ZERO, Watch.Growth.never())
        System.out.println("Read tweets from the JSON File");

        // Parse tweets into their java objects
        PCollection<Tweet> tweets = tweetLines.apply("TransformData", ParseJsons.of(Tweet.class))
                .setCoder(SerializableCoder.of(Tweet.class));
        System.out.println("Parse tweets into their java objects");

        // Plot the Trends
        tweets.apply("SetTimeStamps", WithTimestamps.of(
            tweet -> new Instant(tweet.getCreatedAt().toEpochMilli())
        ))
            .apply("FixedWindowsOfOneDay", Window.into(FixedWindows.of(Duration.standardDays(1))))
            .apply("CheckTrend", ParDo.of(new CheckMultipleTrendSingleProducer(Arrays.asList(trends))))
            .apply("CountPerElement", Count.perElement())
            .apply("ConvertCountIntoOutputString", ParDo.of(new PrintKV(true)))
            .apply(TextIO.write().to(pipelineOptions.getOutputFile()).withNumShards(1).withWindowedWrites());
        System.out.println("Check Trends with Tumbling Windows");

        pipeline.run().waitUntilFinish();

        return;
    }

}
