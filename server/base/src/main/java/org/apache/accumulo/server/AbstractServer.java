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
package org.apache.accumulo.server;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.OptionalInt;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.thrift.MetricSource;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.mem.LowMemoryDetector;
import org.apache.accumulo.server.metrics.MetricServiceHandler;
import org.apache.accumulo.server.metrics.ProcessMetrics;
import org.apache.accumulo.server.security.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.micrometer.core.instrument.MeterRegistry;

public abstract class AbstractServer implements AutoCloseable, MetricsProducer, Runnable {

  private final ServerContext context;
  protected final String applicationName;
  private final String hostname;
  private final String resourceGroup;
  private final Logger log;
  private final ProcessMetrics processMetrics;
  protected final long idleReportingPeriodMillis;
  private volatile Timer idlePeriodTimer = null;
  private volatile Thread serverThread;
  private volatile Thread verificationThread;

  protected AbstractServer(String appName, ConfigOpts opts,
      Function<SiteConfiguration,ServerContext> serverContextFactory, String[] args) {
    this.applicationName = appName;
    opts.parseArgs(appName, args);
    var siteConfig = opts.getSiteConfiguration();
    this.hostname = siteConfig.get(Property.GENERAL_PROCESS_BIND_ADDRESS);
    this.resourceGroup = getResourceGroupPropertyValue(siteConfig);
    SecurityUtil.serverLogin(siteConfig);
    context = serverContextFactory.apply(siteConfig);
    log = LoggerFactory.getLogger(getClass());
    log.info("Version " + Constants.VERSION);
    log.info("Instance " + context.getInstanceID());
    context.init(appName);
    ClassLoaderUtil.initContextFactory(context.getConfiguration());
    TraceUtil.initializeTracer(context.getConfiguration());
    if (context.getSaslParams() != null) {
      // Server-side "client" check to make sure we're logged in as a user we expect to be
      context.enforceKerberosLogin();
    }
    final LowMemoryDetector lmd = context.getLowMemoryDetector();
    ScheduledFuture<?> future = context.getScheduledExecutor().scheduleWithFixedDelay(
        () -> lmd.logGCInfo(context.getConfiguration()), 0,
        lmd.getIntervalMillis(context.getConfiguration()), MILLISECONDS);
    ThreadPools.watchNonCriticalScheduledTask(future);
    processMetrics = new ProcessMetrics(context);
    idleReportingPeriodMillis =
        context.getConfiguration().getTimeInMillis(Property.GENERAL_IDLE_PROCESS_INTERVAL);
  }

  /**
   * Updates the idle status of the server to set the idle process metric. The server must be idle
   * for multiple calls over a specified period for the metric to reflect the idle state. If the
   * server is busy or the idle period hasn't started, it resets the idle tracking.
   *
   * @param isIdle whether the server is idle
   */
  protected void updateIdleStatus(boolean isIdle) {
    boolean shouldResetIdlePeriod = !isIdle || idleReportingPeriodMillis == 0;
    boolean hasIdlePeriodStarted = idlePeriodTimer != null;
    boolean hasExceededIdlePeriod =
        hasIdlePeriodStarted && idlePeriodTimer.hasElapsed(idleReportingPeriodMillis, MILLISECONDS);

    if (shouldResetIdlePeriod) {
      // Reset idle period and set idle metric to false
      idlePeriodTimer = null;
      processMetrics.setIdleValue(false);
    } else if (!hasIdlePeriodStarted) {
      // Start tracking idle period
      idlePeriodTimer = Timer.startNew();
    } else if (hasExceededIdlePeriod) {
      // Set idle metric to true and reset the start of the idle period
      processMetrics.setIdleValue(true);
      idlePeriodTimer = null;
    }
  }

  protected String getResourceGroupPropertyValue(SiteConfiguration conf) {
    return Constants.DEFAULT_RESOURCE_GROUP_NAME;
  }

  public String getResourceGroup() {
    return resourceGroup;
  }

  /**
   * Run this server in a main thread
   */
  public void runServer() throws Exception {
    final AtomicReference<Throwable> err = new AtomicReference<>();
    serverThread = new Thread(TraceUtil.wrap(this), applicationName);
    serverThread.setUncaughtExceptionHandler((thread, exception) -> err.set(exception));
    serverThread.start();
    serverThread.join();
    if (verificationThread != null) {
      verificationThread.interrupt();
      verificationThread.join();
    }
    Throwable thrown = err.get();
    if (thrown != null) {
      if (thrown instanceof Error) {
        throw (Error) thrown;
      }
      if (thrown instanceof Exception) {
        throw (Exception) thrown;
      }
      throw new IllegalStateException("Weird throwable type thrown", thrown);
    }
  }

  /**
   * Called
   */
  @Override
  public void registerMetrics(MeterRegistry registry) {
    // makes mocking subclasses easier
    if (processMetrics != null) {
      processMetrics.registerMetrics(registry);
    }
    getContext().setMeterRegistry(registry);
  }

  public String getHostname() {
    return hostname;
  }

  public ServerContext getContext() {
    return context;
  }

  public AccumuloConfiguration getConfiguration() {
    return getContext().getConfiguration();
  }

  public String getApplicationName() {
    return applicationName;
  }

  protected MetricServiceHandler createMetricServiceHandler(MetricSource serverType) {
    return new MetricServiceHandler(serverType, getResourceGroup(), context);
  }

  /**
   * Get the ServiceLock for this server process. May return null if called before the lock is
   * acquired.
   *
   * @return lock ServiceLock or null
   */
  public abstract ServiceLock getLock();

  public void startServiceLockVerificationThread() {
    Preconditions.checkState(verificationThread == null,
        "verification thread not null, startServiceLockVerificationThread likely called twice");
    Preconditions.checkState(serverThread != null,
        "server thread is null, no server process is running");
    final long interval =
        getConfiguration().getTimeInMillis(Property.GENERAL_SERVER_LOCK_VERIFICATION_INTERVAL);
    if (interval > 0) {
      verificationThread = Threads.createThread("service-lock-verification-thread",
          OptionalInt.of(Thread.NORM_PRIORITY + 1), () -> {
            while (serverThread.isAlive()) {
              ServiceLock lock = getLock();
              try {
                log.trace(
                    "ServiceLockVerificationThread - checking ServiceLock existence in ZooKeeper");
                if (lock != null && !lock.verifyLockAtSource()) {
                  Halt.halt("Lock verification thread could not find lock", -1);
                }
                // Need to sleep, not yield when the thread priority is greater than NORM_PRIORITY
                // so that this thread does not get immediately rescheduled.
                log.trace(
                    "ServiceLockVerificationThread - ServiceLock exists in ZooKeeper, sleeping for {}ms",
                    interval);
                Thread.sleep(interval);
              } catch (InterruptedException e) {
                if (serverThread.isAlive()) {
                  // throw an Error, which will cause this process to be terminated
                  throw new Error("Sleep interrupted in ServiceLock verification thread");
                }
              }
            }
          });
      verificationThread.start();
    } else {
      log.info("ServiceLockVerificationThread not started as "
          + Property.GENERAL_SERVER_LOCK_VERIFICATION_INTERVAL.getKey() + " is zero");
    }
  }

  @Override
  public void close() {}

}
