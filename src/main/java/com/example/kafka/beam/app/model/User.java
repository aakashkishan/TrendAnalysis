package com.example.kafka.beam.app.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.io.Serializable;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class User implements Serializable {

    private String name;

    @JsonProperty("screen_name")
    private String screenName;

    @JsonProperty("location")
    private String location;

    @Override
    public String toString() {
        return "{" +
                "\"name\":\"" + name + "\"" +
                "," + "\"screen_name\":\"" + screenName + "\"" +
                "," + "\"location\":\"" + location + "\"" +
                "}";
    }

}
