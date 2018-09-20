/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.servicecomb.saga.alpha.core;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.servicecomb.saga.common.EventType.*;

public class TxConsistentService {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final TxEventRepository eventRepository;

  private final List<String> types = Arrays.asList(TxStartedEvent.name(), SagaEndedEvent.name());

  private BlockingDeque<TxEvent> abortEventsDeque;
  private BlockingDeque<Command> commandsDeque;

  public TxConsistentService(TxEventRepository eventRepository,
                             BlockingDeque<TxEvent> abortEventsDeque,
                             BlockingDeque<Command> commandsDeque) {
    this.eventRepository = eventRepository;
    this.abortEventsDeque = abortEventsDeque;
    this.commandsDeque = commandsDeque;
  }

  public boolean handle(TxEvent event) {
    if (types.contains(event.type()) && isGlobalTxAborted(event)) {
      LOG.info("Transaction event {} rejected, because its parent with globalTxId {} was already aborted",
          event.type(), event.globalTxId());
      return false;
    }
    else if(event.type().equals(TxAbortedEvent.name())){
      LOG.info("Add abort event into abortEventDeque event {}", event);
      abortEventsDeque.add(event);
    }
    else{
        if(event.type().equals(TxEndedEvent.name()) && isGlobalTxAbortedAndRetriesIsZero(event.globalTxId())){
            LOG.info("Find Uncompensate TxEndedEvent!");
            commandsDeque.add(new Command(event));
        }
        eventRepository.save(event);
    }

    return true;
  }

  private boolean isGlobalTxAborted(TxEvent event) {
    return !eventRepository.findTransactions(event.globalTxId(), TxAbortedEvent.name()).isEmpty();
  }

  private boolean isGlobalTxAbortedAndRetriesIsZero(String globalTxId){
    return !eventRepository.findIsGlobalAbortByGlobalTxId(globalTxId).isEmpty();
  }
}
