package com.example.kafka.beam.app.services.dofns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class PrintKV extends DoFn<KV<String, Long>, String> {

    public boolean isMultipleTrend;

    public PrintKV(boolean isMultipleTrend) {
        this.isMultipleTrend = isMultipleTrend;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        if(isMultipleTrend) {
            String trendPhrase = context.element().getKey();
            Long trendOccurrence = context.element().getValue();
            context.output(String.format("Term:%s => Count:%s", trendPhrase, trendOccurrence));
        } else {
            String trendProducer = context.element().getKey();
            Long trendOccurrence = context.element().getValue();
            context.output(String.format("Producer:%s => Count:%s", trendProducer, trendOccurrence));
        }
    }

}
