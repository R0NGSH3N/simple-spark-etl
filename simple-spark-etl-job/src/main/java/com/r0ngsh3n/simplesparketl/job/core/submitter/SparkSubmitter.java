package com.r0ngsh3n.simplesparketl.job.core.submitter;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@Slf4j
public class SparkSubmitter {
   private final SparkConfig sparkConfig;
   private final Supplier<SparkLauncher> sparkLauncherSupplier;

   public SparkSubmitter(SparkConfig config){
      this.sparkConfig = config;
      this.sparkLauncherSupplier = SparkLauncher::new;
   }


   public CompletableFuture<String> submit(String... fileName){

      SparkLauncher launcher = sparkLauncherSupplier.get();

      String sparkHome = sparkConfig.getHome();
      String sparkMaster = sparkConfig.getMaster();
      String serviceJar = sparkConfig.getServiceJar();
      String mainClass = sparkConfig.getMainClass();
      List<String> jars = sparkConfig.getJars();
      String blockManagerPort = sparkConfig.getBlockManagerPort();
      String bindingAddress = sparkConfig.getBindingAddress();
      String driverPort = sparkConfig.getDriverPort();

      if(StringUtils.isNotBlank(bindingAddress)){
         launcher.setConf("spark.driver.bindAddress", bindingAddress);
      }

      if(StringUtils.isNotBlank(driverPort)){
         launcher.setConf("spark.driver.port", driverPort);
      }

      if(StringUtils.isNotBlank(blockManagerPort)){
         launcher.setConf("spark.driver.blockManager.port", blockManagerPort);
      }

      if(StringUtils.isNotBlank(sparkConfig.getExecutorMemory())){
         launcher.setConf("spark.executor.memory", sparkConfig.getExecutorMemory());
      }

      if(StringUtils.isNotBlank(sparkConfig.getDriverMemory())){
         launcher.setConf("spark.driver.memory", sparkConfig.getDriverMemory());
      }

      /**
       * default is client mode
       */
      if(StringUtils.isNotBlank(sparkConfig.getDeployMode())){
         launcher.setDeployMode(sparkConfig.getDeployMode());
      }

//      ImmutableList.Builder<String> appArgsBuilder = ImmutableList.<String>builder()
////              .add(fileName)
//              .add(sparkConfig.getJobConfigFileName());

      launcher.setSparkHome(sparkHome)
              .setMaster(sparkMaster)
              .setAppName("ETL-" + sparkConfig.getJobName())
              .setAppResource(serviceJar)
              .setVerbose(true);
//              .addAppArgs(appArgsBuilder.build().toArray(new String[0]));

      if(sparkConfig.isEnableDebug()){
         launcher.setConf("spark.driver.extraJavaOption", "-agentlib:jdwp=transport=dt_sorcket,server=y,suspend=y,address=" + sparkConfig.getDebugPort());
      }

      jars.forEach(launcher::addJar);

      CompletableFuture<String> sparkJobFuture = new CompletableFuture<>();
      try{
         launcher.startApplication(new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle handle) {
               SparkAppHandle.State state = handle.getState();
               log.info("state {} isFinal {}", state, state.isFinal());
               if(state.isFinal()){
                  sparkJobFuture.complete(state.name());
               }
            }

            @Override
            public void infoChanged(SparkAppHandle handle) {
                log.info("info changed, appId is: {}", handle.getAppId());
            }
         });
      }catch (IOException ioe){
         log.error("failed to submit spark job", ioe);
         throw new RuntimeException("failed to submit spark job " + ioe.getMessage() );
      }

      return sparkJobFuture;
   }



}
