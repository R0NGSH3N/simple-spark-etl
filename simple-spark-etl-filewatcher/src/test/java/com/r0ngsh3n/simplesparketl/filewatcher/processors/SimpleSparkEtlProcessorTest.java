package com.r0ngsh3n.simplesparketl.filewatcher.processors;

import com.r0ngsh3n.simplesparketl.job.config.SimpleSparkEtlJobConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

public class SimpleSparkEtlProcessorTest {

    @Test
    public void testRun(){

        SimpleSparkEtlJobConfig simpleSparkEtlJobConfig = SimpleSparkEtlJobConfig.loadConfig();
        SimpleSparkEtlProcessor processor = new SimpleSparkEtlProcessor(simpleSparkEtlJobConfig);
        assertEquals(processor.getExtractConfigDirectoryList().size(), 2);

        SimpleSparkEtlProcessor spy = spy(processor);
        doNothing().when(spy).runJob(null);

        spy.run();
    }

}
