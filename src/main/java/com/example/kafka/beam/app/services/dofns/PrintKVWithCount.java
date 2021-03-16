package com.example.kafka.beam.app.services.dofns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class PrintKVWithCount extends DoFn<KV<KV<String, String>, Long>, String> {

    @ProcessElement
    public void processElemenrt(ProcessContext context) {
        String trendPhrase = context.element().getKey().getKey();
        String trendProducer = context.element().getKey().getValue();
        Long trendOccurrence = context.element().getValue();
        context.output(String.format("Term:%s => Producer:%s => Count:%s", trendPhrase, trendProducer, trendOccurrence));
    }

}
