package com.example.kafka.beam.app.cli;

import com.example.kafka.beam.app.services.impl.kafka.KafkaTrendAnalyser;
import com.example.kafka.beam.app.services.impl.TwitterProducer;
import com.example.kafka.beam.app.services.impl.text.MultipleProducerMultipleTrendPlotter;
import com.example.kafka.beam.app.services.impl.text.MultipleProducerSingleTrendPlotter;
import com.example.kafka.beam.app.services.impl.text.SingleProducerMultipleTrendPlotter;
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
import java.io.FileWriter;
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

    public void loadTopicsDummy() {
        this.topics.add("cnn");
        this.topics.add("bbcworld");
        this.topics.add("washingtonpost");
        this.topics.add("huffpost");
        this.topics.add("nytimes");
        this.topics.add("foxnews");
    }

    public void writeToTopics() throws IOException {
        String filePath = String.format("%s%s%s", PathUtils.BASE_PATH, PathUtils.TOPICS, PathUtils.CSV_EXTENSION);
        System.out.println(filePath);
        BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath));
        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
        csvPrinter.printRecord(this.topics);
        printTopics();
        System.out.println("Successful Write");
    }

    public void printTopics() {
        for(String topic: this.topics) {
            System.out.println(topic);
        }
    }

    /* Solely for Twitter Analysis */
    @ShellMethod(key="kb-add-term", value="Add twitter track term and load the data onto Apache Kafka")
    public boolean addTrackingTermAndLoadToKafka(@ShellOption("--term") String term) throws IOException {
        boolean isAddedAndLoaded = false;
        if(term != null) {
            twitterProducer.setTerm(term);

            FileWriter writer = new FileWriter("./terms.csv", true);
            writer.write(String.format("%s,\n", term));
            writer.close();
            log.info(String.format("Term=%s added to file", term));

            isAddedAndLoaded = twitterProducer.loadTwiiterDataToKafka();
        }
        return isAddedAndLoaded;
    }

//    @ShellMethod(key="kb-add-topic", value="Add twitter topic and the load the tweets onto Apache Kafka")
//    public boolean addTopicAndLoadToKafka(@ShellOption("--topic") String topic) throws IOException, InterruptedException, CsvValidationException {
//        boolean isAddedAndLoaded = false;
//        if(topic != null) {
////            this.loadTopics();
////            this.topics.add(topic);
//            new KafkaTopicProducer().writeTweetsToKafka(topic);
////            writeToTopics();
//            isAddedAndLoaded = true;
//        }
//        return isAddedAndLoaded;
//    }

    @ShellMethod(key="kb-trend-compare-window", value="Show Trend Comparison between the news agencies with windowing")
    public boolean checkTrendComparisonWithWindowing(@ShellOption(value = "--trends", arity = 5) String[] trends) throws IOException, CsvValidationException {
        boolean isGenerated = false;
        for(String trend: trends) {
            System.out.println(trend);
        }
        if(trends != null) {
            this.loadTopicsDummy();
            printTopics();
            new KafkaTrendAnalyser().getTrendComparisonWithWindowing(trends, topics);
            isGenerated = true;
        }
        return isGenerated;
    }

    @ShellMethod(key="kb-trend-compare", value="Show Trend Comparison between the news agencies with windowing")
    public boolean checkTrendComparison(@ShellOption(value = "--trends", arity = 5) String[] trends) throws IOException, CsvValidationException {
        boolean isGenerated = false;
        for(String trend: trends) {
            System.out.println(trend);
        }
        if(trends != null) {
            this.loadTopicsDummy();
            printTopics();
            new KafkaTrendAnalyser().getTrendComparisonWithSplittableWindowing(trends, topics);
            isGenerated = true;
        }
        return isGenerated;
    }

    /*
     * ===========================
     * =       TEXT-BEAM         =
     * ===========================
     */
    @ShellMethod(key="tb-sp-mt-plot", value="Single Producer Multiple Trend Plotter")
    public void performTrendPlotterForSingleProducerMultipleTrend(@ShellOption(arity = 3) String[] trends) {
        new SingleProducerMultipleTrendPlotter().plotSingleTwitterTrend(trends);
    }

    @ShellMethod(key="tb-mp-st-plot", value="Multiple Producer Single Trend Plotter")
    public void performTrendPlotterForMultipleProducerSingleTrend(@ShellOption("--trend") String trend) {
        new MultipleProducerSingleTrendPlotter().plotMultipleProducerSingleTwitterTrend(trend);
    }

    @ShellMethod(key="tb-mp-mt-plot", value="Multiple Producer Multiple Trend Plotter")
    public void performTrendPlotterForMultipleProducerMultipleTrend(@ShellOption(arity = 3) String[] trends) {
        new MultipleProducerMultipleTrendPlotter().plotMultipleProducerMultipleTwitterTrend(trends);
    }

}
