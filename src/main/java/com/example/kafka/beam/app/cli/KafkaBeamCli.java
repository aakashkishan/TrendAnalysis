package com.example.kafka.beam.app.cli;

import com.example.kafka.beam.app.services.impl.kafka.KafkaTopicProducer;
import com.example.kafka.beam.app.services.impl.kafka.KafkaTrendAnalyser;
import com.example.kafka.beam.app.services.impl.TwitterProducer;
import com.example.kafka.beam.app.services.impl.trend.MultipleProducerMultipleTrendPlotter;
import com.example.kafka.beam.app.services.impl.trend.MultipleProducerSingleTrendPlotter;
import com.example.kafka.beam.app.services.impl.trend.SingleProducerMultipleTrendPlotter;
import com.example.kafka.beam.app.utils.PathUtils;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@ShellComponent
@Slf4j
public class KafkaBeamCli {

    private final TwitterProducer twitterProducer;

    private List<String> topics = new ArrayList<>();

    public KafkaBeamCli(TwitterProducer twitterProducer) throws IOException, CsvValidationException {
        this.twitterProducer = twitterProducer;
        loadTopics();
    }

    public void loadTopics() throws IOException, CsvValidationException {
        String filePath = String.format("%s%s%s", PathUtils.BASE_PATH, PathUtils.TOPICS, PathUtils.CSV_EXTENSION);
        CSVReader csvReader = new CSVReader(new FileReader(filePath));
        String[] line;
        while((line = csvReader.readNext()) != null) {
            this.topics.add(line[0]);
        }
        printTopics();
        System.out.println("Successful Load");
    }

    public void printTopics() {
        for(String topic: this.topics) {
            System.out.println(topic);
        }
    }

    /* Solely for Twitter Analysis */
    @ShellMethod(key="kb-add-topics", value="Add Twitter Streams to Kafka")
    public void addTwitterStreamsToKafka() throws IOException {
        new KafkaTopicProducer().writeTweetTopicsToKafka();
    }

    /*
     * ===========================
     * =       KAFKA-BEAM         =
     * ===========================
     */
    @ShellMethod(key="kb-sp-mt-plot", value="Single Producer Multiple Trend Plotter")
    public void performTrendPlotterForSingleProducerMultipleTrend(@ShellOption(arity = 3) String[] trends) {
        new SingleProducerMultipleTrendPlotter().plotSingleProducerMultipleTwitterTrend(trends);
    }

    @ShellMethod(key="kb-mp-st-plot", value="Multiple Producer Single Trend Plotter")
    public void performTrendPlotterForMultipleProducerSingleTrend(@ShellOption("--trend") String trend) {
        new MultipleProducerSingleTrendPlotter().plotMultipleProducerSingleTwitterTrend(trend);
    }

    @ShellMethod(key="kb-mp-mt-plot", value="Multiple Producer Multiple Trend Plotter")
    public void performTrendPlotterForMultipleProducerMultipleTrend(@ShellOption(arity = 3) String[] trends) {
        new MultipleProducerMultipleTrendPlotter().plotMultipleProducerMultipleTwitterTrend(trends);
    }

    @ShellMethod(key="kb-plot", value="Multiple Producer Multiple Trend Plotter")
    public void performTrendPlotterForMultipleProducerMultipleTrends() {
        new KafkaTrendAnalyser().getTrendComparisonWithWindowing();
    }

}
