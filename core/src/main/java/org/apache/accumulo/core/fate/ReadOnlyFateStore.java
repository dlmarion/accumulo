/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.fate;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Read only access to a Transaction Store.
 *
 * A transaction consists of a number of operations. Instances of this class may check on the queue
 * of outstanding transactions but may neither modify them nor create new ones.
 */
public interface ReadOnlyFateStore<T> {

  /**
   * Possible operational status codes. Serialized by name within stores.
   */
  enum TStatus {
    /** Unseeded transaction */
    NEW,
    /** Transaction that is executing */
    IN_PROGRESS,
    /** Transaction has failed, and is in the process of being rolled back */
    FAILED_IN_PROGRESS,
    /** Transaction has failed and has been fully rolled back */
    FAILED,
    /** Transaction has succeeded */
    SUCCESSFUL,
    /** Unrecognized or unknown transaction state */
    UNKNOWN,
    /** Transaction that is eligible to be executed */
    SUBMITTED;
  }

  /**
   * Reads the data related to fate transaction without reserving it.
   */
  ReadOnlyFateTxStore<T> read(FateId fateId);

  /**
   * Storage for an individual fate transaction
   */
  interface ReadOnlyFateTxStore<T> {

    /**
     * Get the current operation for the given transaction id.
     *
     * Caller must have already reserved tid.
     *
     * @return a read-only view of the operation
     */
    ReadOnlyRepo<T> top();

    /**
     * Get all operations on a transactions stack. Element 0 contains the most recent operation
     * pushed or the top.
     */
    List<ReadOnlyRepo<T>> getStack();

    /**
     * Get the state of a given transaction.
     *
     * Caller must have already reserved tid.
     *
     * @return execution status
     */
    TStatus getStatus();

    Optional<FateKey> getKey();

    /**
     * Wait for the status of a transaction to change
     *
     * @param expected a set of possible statuses we are interested in being notified about. may not
     *        be null.
     * @return execution status.
     */
    TStatus waitForStatusChange(EnumSet<TStatus> expected);

    /**
     * Retrieve transaction-specific information.
     *
     * Caller must have already reserved tid.
     *
     * @param txInfo name of attribute of a transaction to retrieve.
     */
    Serializable getTransactionInfo(Fate.TxInfo txInfo);

    /**
     * Retrieve the creation time of a FaTE transaction.
     *
     * @return creation time of transaction.
     */
    long timeCreated();

    /**
     * @return the id of the FATE transaction
     */
    FateId getID();
  }

  interface FateIdStatus {
    FateId getFateId();

    Optional<FateStore.FateReservation> getFateReservation();

    TStatus getStatus();

    Optional<Fate.FateOperation> getFateOperation();
  }

  /**
   * list all transaction ids in store.
   *
   * @return all outstanding transactions, including those reserved by others.
   */
  Stream<FateIdStatus> list();

  /**
   * list all transaction ids in store that have a current status that is in the provided set
   *
   * @return all outstanding transactions, including those reserved by others.
   */
  Stream<FateIdStatus> list(EnumSet<TStatus> statuses);

  /**
   * list transaction in the store that have a given fate key type.
   */
  Stream<FateKey> list(FateKey.FateKeyType type);

  /**
   * @return a map of the current active reservations with the keys being the transaction that is
   *         reserved and the value being the value stored to indicate the transaction is reserved.
   */
  Map<FateId,FateStore.FateReservation> getActiveReservations();

  /**
   * Finds all fate ops that are (IN_PROGRESS, SUBMITTED, or FAILED_IN_PROGRESS) and unreserved. Ids
   * that are found are passed to the consumer. This method will block until at least one runnable
   * is found or until the keepWaiting parameter is false. It will return once all runnable ids
   * found were passed to the consumer.
   */
  void runnable(AtomicBoolean keepWaiting, Consumer<FateIdStatus> idConsumer);

  /**
   * Returns true if the deferred map was cleared and if deferred executions are currently disabled
   * because of too many deferred transactions
   *
   * @return true if the map is in a deferred overflow state, else false
   */
  boolean isDeferredOverflow();

  /**
   * @return the current number of transactions that have been deferred
   */
  int getDeferredCount();

  FateInstanceType type();
}
