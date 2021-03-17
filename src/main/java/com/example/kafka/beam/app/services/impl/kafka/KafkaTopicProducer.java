package com.example.kafka.beam.app.services.impl.kafka;

import com.example.kafka.beam.app.model.Tweet;
import com.example.kafka.beam.app.serializer.TweetSerializer;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

@Service
public class KafkaTopicProducer {

    public void writeTweetTopicsToKafka() {

        KafkaTopicProducerPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs("")
                .withValidation().as(KafkaTopicProducerPipelineOptions.class);
        pipelineOptions.setRunner(FlinkRunner.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        // Read tweets from the JSONL File
        PCollection<String> tweetLines = pipeline.apply("ReadLines", TextIO.read()
                .from(pipelineOptions.getInputFile()));
        System.out.println("Read tweets from the JSON File");

        // Parse tweets into their java objects
        PCollection<Tweet> tweets = tweetLines.apply("TransformData", ParseJsons.of(Tweet.class))
                .setCoder(SerializableCoder.of(Tweet.class));
        System.out.println("Parse tweets into their java objects");

        // Convert the PCollection<Tweet> into a PCollection<KV>
        PCollection<KV<String, Tweet>> tweetsKeyValue = tweets.apply("KeyValueData",
                WithKeys.of((SerializableFunction<Tweet, String>) t -> t.getUser().getScreenName()))
                .setCoder(KvCoder.of(SerializableCoder.of(String.class), SerializableCoder.of(Tweet.class)));
        System.out.println("Convert the PCollection<Tweet> into a PCollection<KV>");

        // Push the PCollection<KV> into the Kafka Server
        tweetsKeyValue.apply("WriteData", KafkaIO.<String, Tweet>write()
                .withBootstrapServers(pipelineOptions.getBootstrapServer()).withTopic(pipelineOptions.getKafkaTopic())
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(TweetSerializer.class));
        System.out.println("Push the PCollection<KV> into the Kafka Server");

        pipeline.run().waitUntilFinish();

        return;
    }

}
