package com.example.kafka.beam.app.utils;

public class KafkaConfig {

    public final static String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public final static String TOPIC = "twitter-data";

    public final static String ACKS_CONFIG = "all";

    public static final String COMPRESSION_TYPE = "snappy";

    public final static String MAX_IN_FLIGHT_CONNECTION = "5";

    public static final String RETRIES_CONFIG = Integer.toString(Integer.MAX_VALUE);

    public static final String LINGER_CONFIG = "20";

    public static final String BATCH_SIZE = Integer.toString(32*1024);

}
