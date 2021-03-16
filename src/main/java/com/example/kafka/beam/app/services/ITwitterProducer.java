package com.example.kafka.beam.app.services;

public interface ITwitterProducer<S, T, V> {

    public S getKafkaProducer();

    public T getTwitterClient(V queue);

    public boolean loadTwiiterDataToKafka();

}
