package com.r0ngsh3n.simplesparketl.filewatcher;

import com.r0ngsh3n.simplesparketl.filewatcher.config.SimpleSparkEtlFilWatcherConfig;
import com.r0ngsh3n.simplesparketl.job.config.SparkConfig;

public class ConfigTester {
    public static void main(String[] args) {
        SimpleSparkEtlFilWatcherConfig simpleSparkEtlFilWatcherConfig = SimpleSparkEtlFilWatcherConfig.loadConfig();
        SparkConfig sparkConfig = simpleSparkEtlFilWatcherConfig.getSparkConfig();
//        System.out.println(sparkConfig.getHome());
    }
}
