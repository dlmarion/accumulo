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
package org.apache.accumulo.test.compaction;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService.Client;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.process.thrift.ServerProcessService;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorThriftClientBehaviorIT extends SharedMiniClusterBase {

  public static class DoNothingCoordinator extends CompactionCoordinator
      implements CompactionCoordinatorService.Iface, ServerProcessService.Iface {

    private static final Logger LOG = LoggerFactory.getLogger(DoNothingCoordinator.class);

    private AtomicInteger msgsReceived = new AtomicInteger(0);

    protected DoNothingCoordinator(ServerOpts opts, String[] args) {
      super(opts, args);
    }

    @Override
    public void updateCompactionStatus(TInfo tinfo, TCredentials credentials,
        String externalCompactionId, TCompactionStatusUpdate update, long timestamp)
        throws ThriftSecurityException {
      // This method does nothing, it sits and spins to emulate a backlog
      // updating some internal state. This allows the test to spin up
      // multiple Thrift clients to see what happens to the Thrift client
      // when the total message size is reached in the Thrift server.
      LOG.warn("DoNothingCoordinator.updateCompactionStatus received msg: {}",
          msgsReceived.incrementAndGet());
      while (true) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.error("DoNothingCompactor.compactionCompleted interrupted", e);
        }
      }
    }

    public static void main(String[] args) throws Exception {
      try (DoNothingCoordinator coordinator = new DoNothingCoordinator(new ServerOpts(), args)) {
        coordinator.runServer();
      }
    }
  }

  private class StatusUpdateSender implements Runnable {

    private final ServerContext ctx;
    private final String msg;

    private StatusUpdateSender(ServerContext ctx, String msg) {
      this.ctx = ctx;
      this.msg = msg;
    }

    @Override
    public void run() {
      try {
        var coordinatorHost = ExternalCompactionUtil.findCompactionCoordinator(ctx);
        if (coordinatorHost.isEmpty()) {
          throw new TTransportException(
              "Unable to get CompactionCoordinator address from ZooKeeper");
        }
        Client client =
            ThriftUtil.getClient(ThriftClientTypes.COORDINATOR, coordinatorHost.orElseThrow(), ctx);
        try {
          TCompactionStatusUpdate update =
              new TCompactionStatusUpdate(TCompactionState.STARTED, msg, -1, -1, -1, 0);
          client.updateCompactionStatus(TraceUtil.traceInfo(), ctx.rpcCreds(),
              "FakeExternalCompactionId", update, System.currentTimeMillis());
          // This call should never return. Fail if it does
          throw new RuntimeException("Call to updateCompactionStatus returned");
        } finally {
          ThriftUtil.returnClient(client, ctx);
        }
      } catch (TException e) {
        throw new RuntimeException("Exception received sending status update", e);
      }
    }
  }

  public static class CoordinatorThriftClientBehaviorConfig
      implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.setNumCompactors(0);
      cfg.setNumScanServers(0);
      cfg.setNumTservers(1);
      cfg.setProperty(Property.GENERAL_RPC_TIMEOUT, "0"); // client socket should never timeout
      cfg.setProperty(Property.RPC_MAX_MESSAGE_SIZE, Integer.toString(3 * 1024 * 1024)); // 3MB
      cfg.setServerClass(ServerType.COMPACTION_COORDINATOR, DoNothingCoordinator.class);
    }
  }

  @BeforeAll
  public static void beforeTests() throws Exception {
    startMiniClusterWithConfig(new CoordinatorThriftClientBehaviorConfig());
  }

  @Test
  public void testThriftClientMaxMessageSizeBehavior() throws Exception {

    ServerContext ctx = getCluster().getServerContext();

    getCluster().getClusterControl().start(ServerType.COMPACTION_COORDINATOR);

    // wait for coordinator address to be populated
    Wait.waitFor(() -> ExternalCompactionUtil.findCompactionCoordinator(ctx).isPresent());

    // TCompactionStatusUpdate takes a String for a msg. Let's make it large, but less
    // than the RPC_MAX_MESSAGE_SIZE.
    StringBuilder buf = new StringBuilder(1_000_000);
    IntStream.rangeClosed(0, 1_000_000).forEach(i -> buf.append('a'));
    String msg = buf.toString();

    ExecutorService es = Executors.newFixedThreadPool(10);
    Future<?> f1 = es.submit(new StatusUpdateSender(ctx, msg));
    Future<?> f2 = es.submit(new StatusUpdateSender(ctx, msg));
    Future<?> f3 = es.submit(new StatusUpdateSender(ctx, msg));
    Future<?> f4 = es.submit(new StatusUpdateSender(ctx, msg));

    // The test should sit here and ultimately time out after 10 minutes
    while (!f1.isDone() && !f2.isDone() && !f3.isDone() && !f4.isDone()) {
      Thread.sleep(1000);
    }

    // At this point you will see 4 tcp connections to the Coordinator
    // with 1 of them having a good amount of data on the receive queue.
    // Looking at the coordinator log, you will see that 3 msgs have been
    // received.

    // If GENERAL_RPC_TIMEOUT is not set to zero, then the
    // Thrift clients would timeout. In the Compactor these Thrift
    // clients are in a RetryableThriftCall that will likely send
    // multiple requests to the Coordinator.

    f1.get();
    f2.get();
    f3.get();
    f4.get();

    fail("Never should have reached here");
  }

}
