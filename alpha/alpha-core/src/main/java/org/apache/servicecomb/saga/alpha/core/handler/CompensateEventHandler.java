package org.apache.servicecomb.saga.alpha.core.handler;

import org.apache.servicecomb.saga.alpha.core.*;

import java.util.concurrent.BlockingDeque;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.servicecomb.saga.common.EventType.TxStartedEvent;

/**
 * 1. Get Command from commandsDeque
 * 2. Call compensate method
 * 3. update command status
 */
public class CompensateEventHandler extends Handler {

    private CommandRepository commandRepository;
    private final OmegaCallback omegaCallback;
    private BlockingDeque<Command> commandsDeque;

    public CompensateEventHandler(BlockingDeque<Command> commandsDeque,
                                  CommandRepository commandRepository,
                                  OmegaCallback omegaCallback){
        this.commandsDeque = commandsDeque;
        this.commandRepository = commandRepository;
        this.omegaCallback = omegaCallback;
    }

    public void run(){
        scheduler.scheduleWithFixedDelay(
                () -> {
                    handle();
                },
                0,
                scheduleInternal,
                MILLISECONDS);
    }
    @Override
    public void handle() {
        Command command = null;
        try {
            //if there are no elements ,it will block
            command = commandsDeque.take();

            omegaCallback.compensate(txStartedEventOf(command));
            LOG.info("Compensating transaction with globalTxId {} and localTxId {}",
                    command.globalTxId(),
                    command.localTxId());

            commandRepository.updateStatusByGlobalTxIdAndLocalTxId(command,
                    TaskStatus.NEW.name(), TaskStatus.PENDING.name());
        } catch (InterruptedException e) {

            if (null != command) {
                LOG.error("Take uncompensate event fail {}", e);
                commandsDeque.addFirst(command);
            }
            else{
                LOG.error("Compensating transaction fail {}", command);
            }
            Thread.currentThread().interrupt();
        } catch(Exception e){
            LOG.error("Compensating transaction fail {}", command);
        }

    }

    private TxEvent txStartedEventOf(Command command) {
        return new TxEvent(
                command.serviceName(),
                command.instanceId(),
                command.globalTxId(),
                command.localTxId(),
                command.parentTxId(),
                TxStartedEvent.name(),
                command.compensationMethod(),
                command.payloads()
        );
    }
}

