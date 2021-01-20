package com.r0ngsh3n.simplesparketl.core.extractor;

import com.r0ngsh3n.simplesparketl.core.JobContext;

public interface Extractor<T> {

    public void extract(JobContext<T> jobContext);
}
