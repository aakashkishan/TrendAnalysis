package com.example.kafka.beam.app.services.impl.trend;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface MultipleProducerSingleTrendPlotterPipelineOptions extends PipelineOptions {

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

    @Description("Path of the file to write")
    @Validation.Required
    @Default.String("streaming/aggregate/multiple-producer-single-trend/trend.txt")
    String getOutputFile();

    void setOutputFile(String filePath);

}
