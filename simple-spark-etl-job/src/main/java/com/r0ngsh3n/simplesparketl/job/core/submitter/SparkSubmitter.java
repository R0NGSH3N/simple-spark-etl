package com.r0ngsh3n.simplesparketl.job.core.submitter;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.r0ngsh3n.simplesparketl.job.config.SimpleSparkEtlJobConfig;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@Slf4j
public class SparkSubmitter {
    private final SimpleSparkEtlJobConfig jobConfig;
    private final Supplier<SparkLauncher> sparkLauncherSupplier;

    public SparkSubmitter(SimpleSparkEtlJobConfig config) {
        this.jobConfig = config;
        this.sparkLauncherSupplier = SparkLauncher::new;
    }


    public CompletableFuture<String> submit(String... arguments) {

        Arrays.asList(arguments).forEach(e -> log.info(" :Arguments in submitter: " + e));


        SparkLauncher launcher = sparkLauncherSupplier.get();


        SimpleSparkEtlJobConfig.SparkConfig sparkConfig = this.jobConfig.getSparkConfig();

        String sparkHome = sparkConfig.getHome();
        String sparkMaster = sparkConfig.getMaster();
        String serviceJar = sparkConfig.getServiceJar();
        String mainClass = sparkConfig.getMainClass();
        List<String> jars = sparkConfig.getJars();


        String[] args = Splitter.on(" ").trimResults().splitToList(arguments[0]).toArray(new String[0]);
        ImmutableList.Builder<String> appArgsBuilder = ImmutableList.<String>builder()
                .add(args)
                .add(sparkConfig.getJobConfigFileName());


        launcher.setSparkHome(sparkHome)
                .setMaster(sparkMaster)
                .setAppName("ETL-" + sparkConfig.getJobName() + "-" + System.currentTimeMillis())
                .setMainClass(mainClass)
                .setAppResource(serviceJar)
                .setVerbose(true)
                .addAppArgs(appArgsBuilder.build().toArray(new String[0]));

        if (sparkConfig.isEnableDebug()) {
            launcher.setConf("spark.driver.extraJavaOption", "-agentlib:jdwp=transport=dt_sorcket,server=y,suspend=y,address=" + sparkConfig.getDebugPort());
        }

        if(!CollectionUtils.isEmpty(jars)){
            jars.forEach(launcher::addJar);
        }


        CompletableFuture<String> sparkJobFuture = new CompletableFuture<>();
        try {
            launcher.startApplication(new SparkAppHandle.Listener() {
                @Override
                public void stateChanged(SparkAppHandle handle) {
                    SparkAppHandle.State state = handle.getState();
                    log.info("state {} isFinal {}", state, state.isFinal());
                    if (state.isFinal()) {
                        sparkJobFuture.complete(state.name());
                    }
                }

                @Override
                public void infoChanged(SparkAppHandle handle) {
                    log.info("info changed, appId is: {}", handle.getAppId());
                }
            });
        } catch (IOException ioe) {
            log.error("failed to submit spark job", ioe);
            throw new RuntimeException("failed to submit spark job " + ioe.getMessage());
        }

        return sparkJobFuture;
    }

}
