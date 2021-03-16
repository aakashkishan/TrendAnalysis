package com.example.kafka.beam.app.serializer;

import com.example.kafka.beam.app.model.Tweet;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class TweetDeserializer implements Deserializer<Tweet> {

    @Override
    public Tweet deserialize(String arg0, byte[] devBytes) {
        ObjectMapper mapper = new ObjectMapper();
        Tweet tweet = null;
        try {
            tweet = mapper.readValue(devBytes, Tweet.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return tweet;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
        // TODO Auto-generated method stub
    }

}
