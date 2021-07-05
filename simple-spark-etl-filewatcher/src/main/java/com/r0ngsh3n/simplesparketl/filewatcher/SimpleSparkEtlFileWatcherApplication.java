package com.r0ngsh3n.simplesparketl.filewatcher;

import com.r0ngsh3n.simplesparketl.filewatcher.processors.SimpleSparkEtlProcessor;
import com.r0ngsh3n.simplesparketl.job.config.SimpleSparkEtlJobConfig;

public class SimpleSparkEtlFileWatcherApplication {

    public static void main(String[] args) {
        SimpleSparkEtlJobConfig simpleSparkEtlJobConfig = SimpleSparkEtlJobConfig.loadConfig();
        SimpleSparkEtlProcessor processor = new SimpleSparkEtlProcessor(simpleSparkEtlJobConfig);
        processor.run();

    }
}

