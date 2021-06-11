package com.r0ngsh3n.simplesparketl.filewatcher;

import com.r0ngsh3n.simplesparketl.filewatcher.config.SimpleSparkEtlFilWatcherConfig;
import com.r0ngsh3n.simplesparketl.filewatcher.processors.SimpleSparkEtlProcessor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleSparkEtlFileWatcherApplication {

    public static void main(String[] args) {
        SimpleSparkEtlFilWatcherConfig simpleSparkEtlFilWatcherConfig = SimpleSparkEtlFilWatcherConfig.loadConfig();
//        SparkConfig sparkConfig = (SparkConfig) this.applicationContext.getBean("clusterSparkConfig");
        SimpleSparkEtlProcessor processor = new SimpleSparkEtlProcessor(simpleSparkEtlFilWatcherConfig);
        processor.run();
    }
}

