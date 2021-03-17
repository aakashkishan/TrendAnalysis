package com.example.kafka.beam.app.services.dofns;

import com.example.kafka.beam.app.model.Tweet;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CheckMultipleTrendMultipleProducer extends DoFn<Tweet, KV<String, String>> {

    public Map<String, Iterable<String>> trendProducerMap = new HashMap<String, Iterable<String>>();

    public List<String> trends = new ArrayList<>();

    public CheckMultipleTrendMultipleProducer(List<String> trends) {
        this.trends = trends;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        Tweet tweet = context.element();
        String[] tweetWords = tweet.getFullText().toLowerCase().split("[ \\-.]");
        for(String trend: trends) {
            for(String tweetWord: tweetWords) {
                if(tweetWord.equalsIgnoreCase(trend)) {
                    System.out.println(String.format("Term:%s => Producer:%s", trend, tweet.getUser().getScreenName()));
                    context.output(KV.of(trend, tweet.getUser().getScreenName()));
                }
            }
        }
    }

}
