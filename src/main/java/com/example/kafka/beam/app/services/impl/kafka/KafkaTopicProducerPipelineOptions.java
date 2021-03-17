package com.example.kafka.beam.app.services.impl.kafka;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface KafkaTopicProducerPipelineOptions extends PipelineOptions {

    @Description("Path of the files to read")
    @Default.String("streaming/*.jsonl")
    String getInputFile();

    void setInputFile(String filePath);

    @Description("Kafka Bootstrap Server")
    @Validation.Required
    @Default.String("127.0.0.1:9092")
    String getBootstrapServer();

    void setBootstrapServer(String bootstrapServer);

    @Description("Kafka Topic")
    @Validation.Required
    @Default.String("news")
    String getKafkaTopic();

    void setKafkaTopic(String topic);

}
