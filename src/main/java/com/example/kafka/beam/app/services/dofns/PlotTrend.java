package com.example.kafka.beam.app.services.dofns;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class PlotTrend extends DoFn<KV<String, Long>, KV<String, Long>> {

    @ProcessElement
    public void processElement(ProcessContext context) {
        return;
    }

}
