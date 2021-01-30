package com.r0ngsh3n.simplesparketl.job;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.r0ngsh3n.simplesparketl.job.config.JobConfigModuleBuilder;
import com.r0ngsh3n.simplesparketl.job.core.JobRunner;
import com.r0ngsh3n.simplesparketl.job.module.JobRunnerConfigModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;

@Slf4j
public final class SimpleSparkEtlJobApplication {
    private SimpleSparkEtlJobApplication() {
        //disable construct
    }

    public static void main(String[] args) {
        //read command option from command line
        log.info("args: {}", (Object)args);

        Options options = new Options();
        //load client customized option
        Option etlOption = new Option(null, "etl", true, null);

        options.addOption(etlOption);

        String[] argsList = null;
        try {
            CommandLine line = new DefaultParser().parse(options, args);
            argsList = line.getArgs();
        } catch (ParseException e) {
            printHelp(options);
            System.exit(-1);
        }

        if(argsList == null || argsList.length != 1){
           printHelp(options);
           System.exit(-1);
        }

        //use Guice.createInjector to create injector
        Injector injector = Guice.createInjector(
                new JobConfigModuleBuilder().setConfigFile(argsList[1]).build()
        );

        JobRunner jobRunner = injector.getInstance(JobRunner.class);
        jobRunner.run();
    }

    private static void printHelp(Options options){
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("SimpleSparkEtlJobApplication [options] configFileName ", options);
    }
}

