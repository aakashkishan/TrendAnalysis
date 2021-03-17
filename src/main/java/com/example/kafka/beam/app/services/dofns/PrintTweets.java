package com.example.kafka.beam.app.services.dofns;

import com.example.kafka.beam.app.model.Tweet;
import org.apache.beam.sdk.transforms.SimpleFunction;

public class PrintTweets extends SimpleFunction<Tweet, Tweet> {

    @Override
    public Tweet apply(Tweet tweet) {
        System.out.println(String.format("Tweet: %s", tweet.toString()));
        return tweet;
    }

}
