## Trend Plotter
---
> A Spring Shell Application that uses Apache Beam to plot the news trends of popular news channels.

## Contents
1. [Features](#Features)
2. [Dependencies](#Dependencies)
3. [Install & Run](#Installation)

## Features
1. Kafka Topic Producer with Twitter Streaming API source
2. Kafka Topic Consumer using Apache Beam
3. Trend Plotting transformations using DoFns and SimpleFunctions

## Dependencies
* Spring Shell
* Apache Beam
* Apache Kafka
* Twarc
* Apache Flink Runner

### Installation

Build the project and run it
```bash
$ mvn clean package
```

Once the Spring shell is launched, Run the various commands on the spring shell:-
1. Publish all the Twitter channels to Kafka
```bash
$ kb-add-topics
```

2. Trend Plotter for Single Producer and check for Multiple Trends
```bash
$ kb-sp-mt-plot <trend-1> <trend-2> <trend-3>
```

3. Trend Plotter for Multiple Producer and check for Single Trends
```bash
$ kb-mp-st-plot --trend <trend>
```

4. Trend Plotter for Multiple Producer and check for Multiple Trends
```bash
$ kb-mp-mt-plot <trend-1> <trend-2> <trend-3>
```
