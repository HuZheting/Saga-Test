package org.apache.servicecomb.saga.alpha.core.handler;

import org.apache.servicecomb.saga.alpha.core.TxEvent;
import org.apache.servicecomb.saga.alpha.core.TxEventRepository;
import org.apache.servicecomb.saga.alpha.core.TxTimeout;
import org.apache.servicecomb.saga.alpha.core.TxTimeoutRepository;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingDeque;


import static org.apache.servicecomb.saga.common.EventType.SagaStartedEvent;
import static org.apache.servicecomb.saga.common.EventType.TxAbortedEvent;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.servicecomb.saga.alpha.core.TaskStatus.NEW;
import static org.apache.servicecomb.saga.common.EventType.TxStartedEvent;

/**
 * 1. Find out timeout event
 * 2. Save timeout event
 * 3. Add abort event to abortEventsDeque
 */
public class TimeoutHandler extends Handler {

    private TxTimeoutRepository txTimeoutRepository;
    private TxEventRepository txEventRepository;
    private BlockingDeque<TxEvent> abortEventsDeque;

    public TimeoutHandler(BlockingDeque<TxEvent> abortEventsDeque,
                          TxTimeoutRepository txTimeoutRepository,
                          TxEventRepository txEventRepository){
        this.abortEventsDeque = abortEventsDeque;
        this.txTimeoutRepository = txTimeoutRepository;
        this.txEventRepository = txEventRepository;
    }

    //TODO : thread
    public void run(){
        scheduler.scheduleWithFixedDelay(
                ()->{
                    handle();
                },
                0,
                scheduleInternal,
                MILLISECONDS);
    }


    @Override
    public void handle() {

        txEventRepository.findTimeoutEvents()
                .forEach(event -> {
                    LOG.info("Found timeout event {}", event);
                    txTimeoutRepository.save(txTimeoutOf(event));
                    LOG.info("Saved timeout event {}", event);

                    if(event.type().equals(TxStartedEvent.name())){
                        abortEventsDeque.add(setIsTimeout(abortEventOf(event)));
                    }else{
                        abortEventsDeque.add(abortEventOf(event));
                    }
                    LOG.info("Add timeout event into abortEventDeque {}", event);
                    //txEventRepository.updateIsTimeoutTrue(event.id());
                });
    }

    private TxTimeout txTimeoutOf(TxEvent event) {
        return new TxTimeout(
                event.id(),
                event.serviceName(),
                event.instanceId(),
                event.globalTxId(),
                event.localTxId(),
                event.parentTxId(),
                event.type(),
                event.expiryTime(),
                NEW.name()
        );
    }

    private TxEvent abortEventOf(TxEvent event){
        return new TxEvent(
                event.serviceName(),
                event.instanceId(),
                event.globalTxId(),
                event.localTxId(),
                event.parentTxId(),
                TxAbortedEvent.name(),
                event.compensationMethod(),
                0,
                event.retryMethod(),
                event.retries(),
                event.payloads()
        );
    }

    private TxEvent setIsTimeout(TxEvent event){
        event.setIsTimeoutTrue();

        return event;
    }
}

