package com.example.kafka.beam.app.services.dofns;

import com.example.kafka.beam.app.model.Tweet;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.ArrayList;
import java.util.List;

public class CheckMultipleTrendSingleProducer extends DoFn<Tweet, String> {

    public List<String> searchTerms = new ArrayList<>();

    public CheckMultipleTrendSingleProducer(List<String> term) {
        this.searchTerms = term;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        Tweet tweet = context.element();
        String[] tweetWords = tweet.getFullText().toLowerCase().split("[ \\-.]");

        for(String searchTerm: searchTerms) {
            for(String tweetWord: tweetWords) {
                if(tweetWord.equalsIgnoreCase(searchTerm)) {
                    context.output(searchTerm);
                }
            }
        }
    }

}
