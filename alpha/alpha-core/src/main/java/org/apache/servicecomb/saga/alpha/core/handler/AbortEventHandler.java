package org.apache.servicecomb.saga.alpha.core.handler;

import org.apache.servicecomb.saga.alpha.core.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;

/**
 * 1. Get abort event from abortEventsDeque
 * 2. Save abort event
 * 3. Add command to commandsDeque
 */
import static java.util.concurrent.TimeUnit.MILLISECONDS;


public class AbortEventHandler extends Handler {

    private BlockingDeque<TxEvent> abortEventsDeque;
    private BlockingDeque<Command> commandsDeque;
    private TxEventRepository txEventRepository;
    private CommandRepository commandRepository;


    public AbortEventHandler(BlockingDeque<TxEvent> abortEventsDeque,
                             BlockingDeque<Command> commandsDeque,
                             TxEventRepository txEventRepository,
                             CommandRepository commandRepository){
        this.abortEventsDeque = abortEventsDeque;
        this.commandsDeque = commandsDeque;
        this.txEventRepository = txEventRepository;
        this.commandRepository = commandRepository;
    }

    public void run(){

        scheduler.scheduleWithFixedDelay(
                ()->{
                    handle();
                },
                0,
                scheduleInternal,
                MILLISECONDS
        );

    }

    @Override
    public void handle() {
        TxEvent event = null;
        try {
            event = abortEventsDeque.take();
            LOG.info("From abortEventsDeque get a abort event {}", event);

            txEventRepository.save(event);
            LOG.info("Save abort event {}", event);

            if(1 == event.isTimeout()){
                saveAndAddtoCommandsDeque(event);
                LOG.info("Timeout event {} add to command", event);
            }

            if(event.retries() == 0) {
                Map<String, TxEvent> eventNeedCompensateWithoutDuplicateTxStartEvent = new LinkedHashMap<>();
                //TODO：It is not needed to find TxStartedEvent
                findTxStartedEventsWithMatchingEndedButNotCompensatedEvents(event.globalTxId()).forEach(
                        e -> {
                            eventNeedCompensateWithoutDuplicateTxStartEvent.computeIfAbsent(e.localTxId(), k -> e);
                            txEventRepository.updateFindStatusBySurrogateId(e.id());
                            LOG.info("Update event {} has been finded.", e);
                        }
                );

                eventNeedCompensateWithoutDuplicateTxStartEvent.values().forEach(e -> {
                    LOG.info("Find uncompensate event with same globalTxId {}", e);
                    saveAndAddtoCommandsDeque(e);

                });
            }
        } catch (InterruptedException e) {

            LOG.error("Take abort event fail {} ", e);
            if (null != event) {
                //if command has been stored, it wouldn't be stored repeated
                abortEventsDeque.addFirst(event);
            }
            Thread.currentThread().interrupt();
        }

    }

    private List<TxEvent> findTxStartedEventsWithMatchingEndedButNotCompensatedEvents(String globalTxId){
        return txEventRepository.findStartedEventsWithMatchingEndedButNotCompensatedEvents(globalTxId);
    }

    private void saveAndAddtoCommandsDeque(TxEvent event){
        Command command = new Command(event);
        try {
            commandRepository.save(command);
            LOG.info("Saved compensation command {}", command);
            commandsDeque.add(command);
            LOG.info("Add event to commandsDeque {}", event);
        }
        catch (Exception e){
            LOG.error("Failed to save some command {}", command);
        }
    }

}
