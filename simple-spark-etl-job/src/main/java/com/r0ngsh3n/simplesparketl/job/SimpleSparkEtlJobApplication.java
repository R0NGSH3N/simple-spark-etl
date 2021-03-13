package com.r0ngsh3n.simplesparketl.job;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.r0ngsh3n.simplesparketl.job.config.JobConfigModuleBuilder;
import com.r0ngsh3n.simplesparketl.job.core.jobrunner.JobRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.spark.sql.SparkSession;

@Slf4j
public final class SimpleSparkEtlJobApplication {
    private SimpleSparkEtlJobApplication() {
        //disable construct
    }

    public static void main(String[] args) {
        //read command option from command line
        log.info("args: {}", (Object) args);

        Options options = null;
        String[] argsList = null;
        JobConfigModuleBuilder configModuleBuilder = null;
        try {
            //last argument MUST be the config file path
            configModuleBuilder = new JobConfigModuleBuilder().setConfigFile(args[args.length - 1]);
            options = configModuleBuilder.getOptions();

            CommandLine line = new DefaultParser().parse(options, args);
            configModuleBuilder.parseRunningParams(line);
        } catch (ParseException e) {
            printHelp(options);
            System.exit(-1);
        }

        //use Guice.createInjector to create injector
        try {
            Injector injector = Guice.createInjector(configModuleBuilder.build());

            JobRunner jobRunner = injector.getInstance(JobRunner.class);
            SparkSession spark = SparkSession.builder().getOrCreate();
            jobRunner.run(spark);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("SimpleSparkEtlJobApplication [options] configFileName ", options);
    }
}

