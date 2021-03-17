package com.example.kafka.beam.app.services.dofns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.Objects;

public class PrintKVWithCount extends DoFn<KV<KV<String, String>, Long>, String> {

    @ProcessElement
    public void processElement(ProcessContext context) {
        String trendPhrase = Objects.requireNonNull(context.element().getKey()).getKey();
        String trendProducer = Objects.requireNonNull(context.element().getKey()).getValue();
        Long trendOccurrence = context.element().getValue();
        System.out.println(String.format("Term:%s => Producer:%s => Count:%s", trendPhrase, trendProducer, trendOccurrence));
        context.output(String.format("Term:%s => Producer:%s => Count:%s", trendPhrase, trendProducer, trendOccurrence));
    }

}
