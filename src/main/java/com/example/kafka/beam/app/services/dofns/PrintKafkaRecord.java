package com.example.kafka.beam.app.services.dofns;

import com.example.kafka.beam.app.model.Tweet;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.SimpleFunction;

public class PrintKafkaRecord extends SimpleFunction<KafkaRecord<String, Tweet>, Tweet> {

    @Override
    public Tweet apply(KafkaRecord<String, Tweet> kafkaRecord) {
        Tweet tweet = kafkaRecord.getKV().getValue();
        System.out.println(String.format("Tweet: %s", tweet.toString()));
        return tweet;
    }

}
