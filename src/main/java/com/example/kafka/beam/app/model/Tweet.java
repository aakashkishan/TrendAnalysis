package com.example.kafka.beam.app.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tweet implements Serializable {

    private User user;

    public void setFullText(String fullText) {
        this.fullText = StringUtils.remove(fullText, "\n");
    }

    @JsonProperty("full_text")
    private String fullText;

//    public void setCreatedAt(String createdAt) throws ParseException {
////        final String twitterFormat="EEE MMM dd HH:mm:ss ZZZZZ yyyy";
////        SimpleDateFormat sf = new SimpleDateFormat(twitterFormat);
////        sf.setLenient(true);
////        this.createdAt = sf.parse(createdAt);
//        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss X uuuu", Locale.ROOT);
//        this.createdAt = OffsetDateTime.parse(createdAt, dtf).toInstant();
//    }

    @JsonProperty("created_at")
    private String createdAt;

    @JsonProperty("retweet_count")
    private Long retweetCount;

    @JsonProperty("favorite_count")
    private Long favoriteCount;

    @Override
    public String toString() {
//        final String twitterFormat="EEE MMM dd HH:mm:ss ZZZZZ yyyy";
//        SimpleDateFormat sf = new SimpleDateFormat(twitterFormat);
        return "{" +
                "\"user\":" + user +
                "," + "\"full_text\":\"" + fullText + "\"" +
                "," + "\"created_at\":\"" + createdAt + "\"" +
                "," + "\"retweet_count\":" + retweetCount +
                "," + "\"favorite_count\":" + favoriteCount +
                "}";
    }

}
