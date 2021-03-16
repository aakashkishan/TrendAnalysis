package com.example.kafka.beam.app.services.impl;

import com.example.kafka.beam.app.model.Tweet;
import com.example.kafka.beam.app.services.ITwitterProducer;
import com.example.kafka.beam.app.utils.KafkaConfig;
import com.example.kafka.beam.app.utils.TwitterConfig;
import com.google.gson.Gson;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class TwitterProducer implements ITwitterProducer<KafkaProducer<String, String>, Client, BlockingQueue<String>> {

    private Client client;

    private KafkaProducer<String, String> producer;

    private BlockingQueue<String> queue = new LinkedBlockingQueue<>(30);

    private Gson gson = new Gson();

    @Setter
    @Getter
    private String term;

    public KafkaProducer<String, String> getKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, KafkaConfig.ACKS_CONFIG);
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, KafkaConfig.RETRIES_CONFIG);
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, KafkaConfig.MAX_IN_FLIGHT_CONNECTION);
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, KafkaConfig.COMPRESSION_TYPE);
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, KafkaConfig.LINGER_CONFIG);
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, KafkaConfig.BATCH_SIZE);

        return new KafkaProducer<String, String>(properties);
    }

    public Client getTwitterClient(BlockingQueue<String> queue) {
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endPoint = new StatusesFilterEndpoint();
        endPoint.trackTerms(Arrays.asList(term));
        Authentication auth = new OAuth1(TwitterConfig.CONSUMER_KEYS, TwitterConfig.CONSUMER_SECRETS,
                TwitterConfig.TOKEN, TwitterConfig.SECRET);

        return new ClientBuilder().name("kafka-beam").hosts(hosts).authentication(auth)
                .endpoint(endPoint).processor(new StringDelimitedProcessor(queue)).build();
    }

    public boolean loadTwiiterDataToKafka() {
        final boolean[] isLoaded = {false};
        client = getTwitterClient(queue);
        client.connect();
        producer = getKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            log.info("Application is not stopping!");
            client.stop();
            log.info("Closing producer");
            producer.close();
            log.info("Finished closing");
        }));

        // 3. Send Tweets to Kafka
        while (!client.isDone()) {
            String message = null;
            String timestamp = null;
            try {
                Tweet tweet = gson.fromJson(queue.poll(5, TimeUnit.SECONDS), Tweet.class);
                timestamp = tweet.getFullText();
                message = tweet.toString();
            } catch (InterruptedException intexp) {
                intexp.printStackTrace();
                client.stop();
            }
            if (message != null) {
                log.info(message);
                producer.send(new ProducerRecord<String, String>(term, timestamp, message), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception exp) {
                        isLoaded[0] = true;
                        if (exp != null) {
                            log.error("Some error OR something bad happened", exp);
                        }
                    }
                });
            }
        }

        return isLoaded[0];
    }

}
