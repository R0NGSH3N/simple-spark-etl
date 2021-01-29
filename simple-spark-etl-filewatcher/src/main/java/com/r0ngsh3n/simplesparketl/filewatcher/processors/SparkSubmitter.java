package com.r0ngsh3n.simplesparketl.filewatcher.processors;

import com.r0ngsh3n.simplesparketl.filewatcher.config.SimpleSparkEtlFilWatcherConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.launcher.SparkLauncher;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@Slf4j
public class SparkSubmitter {
   private final SimpleSparkEtlFilWatcherConfig.SparkConfig sparkConfig;
   private final Supplier<SparkLauncher> sparkLauncherSupplier;

   public SparkSubmitter(SimpleSparkEtlFilWatcherConfig config){
      this.sparkConfig = config.getSparkConfig();
      this.sparkLauncherSupplier = SparkLauncher::new;
   }

   public CompletableFuture<String> process(SimpleSparkEtlFilWatcherConfig.ExtractConfig extractConfig, SimpleSparkEtlFilWatcherConfig.LoadConfig loadConfig){

      SparkLauncher launcher = sparkLauncherSupplier.get();

      String sparkHome = sparkConfig.getHome();
      String serviceJar = sparkConfig.getServiceJar();
      String mainClass = sparkConfig.getMainClass();
   }



}
