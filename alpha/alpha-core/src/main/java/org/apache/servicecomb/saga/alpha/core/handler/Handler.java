package org.apache.servicecomb.saga.alpha.core.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public abstract class Handler{

    static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    int scheduleInternal = 5;

    public abstract void handle();

}
