package com.example.kafka.beam.app.services.dofns;

import com.example.kafka.beam.app.model.Tweet;
import org.apache.beam.sdk.transforms.DoFn;

public class CheckSingleTrendMultipleProducer extends DoFn<Tweet, String> {

    public String searchTerm;

    public CheckSingleTrendMultipleProducer(String term) {
        this.searchTerm = term;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        Tweet tweet = context.element();
        String[] tweetWords = tweet.getFullText().toLowerCase().split("[ \\-.]");
        for(String tweetWord: tweetWords) {
            if(tweetWord.equalsIgnoreCase(searchTerm)) {
                System.out.println(String.format("Producer:%s", tweet.getUser().getScreenName()));
                context.output(tweet.getUser().getScreenName());
            }
        }
    }

}
