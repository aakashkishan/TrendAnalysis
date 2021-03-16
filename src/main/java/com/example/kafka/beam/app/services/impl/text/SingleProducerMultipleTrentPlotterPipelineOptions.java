package com.example.kafka.beam.app.services.impl.text;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface SingleProducerMultipleTrentPlotterPipelineOptions extends PipelineOptions {

    @Description("Path of the files to read")
    @Default.String("streaming/*.jsonl")
    String getInputFile();

    void setInputFile(String filePath);

    @Description("Path of the file to write")
    @Validation.Required
    @Default.String("streaming/aggregate/single-producer-multiple-trend/trend.txt")
    String getOutputFile();

    void setOutputFile(String filePath);

}
