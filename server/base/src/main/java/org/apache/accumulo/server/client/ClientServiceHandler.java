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
package org.apache.accumulo.server.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.clientImpl.thrift.ClientService;
import org.apache.accumulo.core.clientImpl.thrift.ConfigurationType;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TDiskUsage;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.TVersionedProperties;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.util.TableDiskUsage;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientServiceHandler implements ClientService.Iface {
  private static final Logger log = LoggerFactory.getLogger(ClientServiceHandler.class);
  protected final ServerContext context;
  protected final AuditedSecurityOperation security;

  public ClientServiceHandler(ServerContext context) {
    this.context = context;
    this.security = context.getSecurityOperation();
  }

  public static TableId checkTableId(ClientContext context, String tableName,
      TableOperation operation) throws ThriftTableOperationException {
    TableOperationExceptionType reason = null;
    try {
      return context.getTableId(tableName);
    } catch (TableNotFoundException e) {
      reason = e.getCause() instanceof NamespaceNotFoundException
          ? TableOperationExceptionType.NAMESPACE_NOTFOUND : TableOperationExceptionType.NOTFOUND;
    }
    throw new ThriftTableOperationException(null, tableName, operation, reason, null);
  }

  public static NamespaceId checkNamespaceId(ClientContext context, String namespaceName,
      TableOperation operation) throws ThriftTableOperationException {
    try {
      return context.getNamespaceId(namespaceName);
    } catch (NamespaceNotFoundException e) {
      throw new ThriftTableOperationException(null, namespaceName, operation,
          TableOperationExceptionType.NAMESPACE_NOTFOUND, null);
    }
  }

  @Override
  public void ping(TCredentials credentials) {
    // anybody can call this; no authentication check
    log.info("I just got pinged!");
  }

  @Override
  public boolean authenticate(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    try {
      return security.authenticateUser(credentials, credentials);
    } catch (ThriftSecurityException e) {
      log.error("ThriftSecurityException", e);
      throw e;
    }
  }

  @Override
  public boolean authenticateUser(TInfo tinfo, TCredentials credentials, TCredentials toAuth)
      throws ThriftSecurityException {
    try {
      return security.authenticateUser(credentials, toAuth);
    } catch (ThriftSecurityException e) {
      log.error("ThriftSecurityException", e);
      throw e;
    }
  }

  @Override
  public void changeAuthorizations(TInfo tinfo, TCredentials credentials, String user,
      List<ByteBuffer> authorizations) throws ThriftSecurityException {
    security.changeAuthorizations(credentials, user, new Authorizations(authorizations));
  }

  @Override
  public void changeLocalUserPassword(TInfo tinfo, TCredentials credentials, String principal,
      ByteBuffer password) throws ThriftSecurityException {
    PasswordToken token = new PasswordToken(password);
    Credentials toChange = new Credentials(principal, token);
    security.changePassword(credentials, toChange);
  }

  @Override
  public void createLocalUser(TInfo tinfo, TCredentials credentials, String principal,
      ByteBuffer password) throws ThriftSecurityException {
    AuthenticationToken token;
    if (context.getSaslParams() != null) {
      try {
        token = new KerberosToken();
      } catch (IOException e) {
        log.warn("Failed to create KerberosToken");
        throw new ThriftSecurityException(e.getMessage(), SecurityErrorCode.DEFAULT_SECURITY_ERROR);
      }
    } else {
      token = new PasswordToken(password);
    }
    Credentials newUser = new Credentials(principal, token);
    security.createUser(credentials, newUser, new Authorizations());
  }

  @Override
  public void dropLocalUser(TInfo tinfo, TCredentials credentials, String user)
      throws ThriftSecurityException {
    security.dropUser(credentials, user);
  }

  @Override
  public List<ByteBuffer> getUserAuthorizations(TInfo tinfo, TCredentials credentials, String user)
      throws ThriftSecurityException {
    return security.getUserAuthorizations(credentials, user).getAuthorizationsBB();
  }

  @Override
  public void grantSystemPermission(TInfo tinfo, TCredentials credentials, String user,
      byte permission) throws ThriftSecurityException {
    security.grantSystemPermission(credentials, user,
        SystemPermission.getPermissionById(permission));
  }

  @Override
  public void grantTablePermission(TInfo tinfo, TCredentials credentials, String user,
      String tableName, byte permission) throws TException {
    TableId tableId = checkTableId(context, tableName, TableOperation.PERMISSION);
    NamespaceId namespaceId;
    try {
      namespaceId = context.getNamespaceId(tableId);
      security.grantTablePermission(credentials, user, tableId, tableName,
          TablePermission.getPermissionById(permission), namespaceId);
    } catch (TableNotFoundException e) {
      throw new TException(e);
    }
  }

  @Override
  public void grantNamespacePermission(TInfo tinfo, TCredentials credentials, String user,
      String ns, byte permission) throws ThriftSecurityException, ThriftTableOperationException {
    NamespaceId namespaceId = checkNamespaceId(context, ns, TableOperation.PERMISSION);
    security.grantNamespacePermission(credentials, user, namespaceId,
        NamespacePermission.getPermissionById(permission));
  }

  @Override
  public void revokeSystemPermission(TInfo tinfo, TCredentials credentials, String user,
      byte permission) throws ThriftSecurityException {
    security.revokeSystemPermission(credentials, user,
        SystemPermission.getPermissionById(permission));
  }

  @Override
  public void revokeTablePermission(TInfo tinfo, TCredentials credentials, String user,
      String tableName, byte permission) throws TException {
    TableId tableId = checkTableId(context, tableName, TableOperation.PERMISSION);
    NamespaceId namespaceId;
    try {
      namespaceId = context.getNamespaceId(tableId);
    } catch (TableNotFoundException e) {
      throw new TException(e);
    }

    security.revokeTablePermission(credentials, user, tableId,
        TablePermission.getPermissionById(permission), namespaceId);
  }

  @Override
  public boolean hasSystemPermission(TInfo tinfo, TCredentials credentials, String user,
      byte sysPerm) throws ThriftSecurityException {
    return security.hasSystemPermission(credentials, user,
        SystemPermission.getPermissionById(sysPerm));
  }

  @Override
  public boolean hasTablePermission(TInfo tinfo, TCredentials credentials, String user,
      String tableName, byte tblPerm)
      throws ThriftSecurityException, ThriftTableOperationException {
    TableId tableId = checkTableId(context, tableName, TableOperation.PERMISSION);
    return security.hasTablePermission(credentials, user, tableId,
        TablePermission.getPermissionById(tblPerm));
  }

  @Override
  public boolean hasNamespacePermission(TInfo tinfo, TCredentials credentials, String user,
      String ns, byte perm) throws ThriftSecurityException, ThriftTableOperationException {
    NamespaceId namespaceId = checkNamespaceId(context, ns, TableOperation.PERMISSION);
    return security.hasNamespacePermission(credentials, user, namespaceId,
        NamespacePermission.getPermissionById(perm));
  }

  @Override
  public void revokeNamespacePermission(TInfo tinfo, TCredentials credentials, String user,
      String ns, byte permission) throws ThriftSecurityException, ThriftTableOperationException {
    NamespaceId namespaceId = checkNamespaceId(context, ns, TableOperation.PERMISSION);
    security.revokeNamespacePermission(credentials, user, namespaceId,
        NamespacePermission.getPermissionById(permission));
  }

  @Override
  public Set<String> listLocalUsers(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    return security.listUsers(credentials);
  }

  private Map<String,String> conf(TCredentials credentials, AccumuloConfiguration conf)
      throws TException {
    conf.invalidateCache();

    Map<String,String> result = new HashMap<>();
    for (Entry<String,String> entry : conf) {
      String key = entry.getKey();
      if (!Property.isSensitive(key)) {
        result.put(key, entry.getValue());
      }
    }
    return result;
  }

  private boolean checkSystemUserAndAuthenticate(TCredentials credentials)
      throws ThriftSecurityException {
    return security.isSystemUser(credentials)
        && security.authenticateUser(credentials, credentials);
  }

  private void checkSystemPermission(TCredentials credentials) throws ThriftSecurityException {
    if (!(checkSystemUserAndAuthenticate(credentials) || security.hasSystemPermission(credentials,
        credentials.getPrincipal(), SystemPermission.SYSTEM))) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }
  }

  private void checkTablePermission(TCredentials credentials, TableId tableId,
      TablePermission tablePermission) throws ThriftSecurityException {
    if (!(checkSystemUserAndAuthenticate(credentials)
        || security.hasSystemPermission(credentials, credentials.getPrincipal(),
            SystemPermission.SYSTEM)
        || security.hasTablePermission(credentials, credentials.getPrincipal(), tableId,
            tablePermission))) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }
  }

  private void checkNamespacePermission(TCredentials credentials, NamespaceId namespaceId,
      NamespacePermission namespacePermission) throws ThriftSecurityException {
    if (!(checkSystemUserAndAuthenticate(credentials)
        || security.hasSystemPermission(credentials, credentials.getPrincipal(),
            SystemPermission.SYSTEM)
        || security.hasNamespacePermission(credentials, credentials.getPrincipal(), namespaceId,
            namespacePermission))) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }
  }

  @Override
  public Map<String,String> getConfiguration(TInfo tinfo, TCredentials credentials,
      ConfigurationType type) throws TException {
    checkSystemPermission(credentials);
    switch (type) {
      case CURRENT:
        context.getPropStore().getCache().remove(SystemPropKey.of());
        return conf(credentials, context.getConfiguration());
      case SITE:
        return conf(credentials, context.getSiteConfiguration());
      case DEFAULT:
        return conf(credentials, context.getDefaultConfiguration());
    }
    throw new IllegalArgumentException("Unexpected configuration type " + type);
  }

  @Override
  public Map<String,String> getSystemProperties(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    checkSystemPermission(credentials);
    return context.getPropStore().get(SystemPropKey.of()).asMap();
  }

  @Override
  public TVersionedProperties getVersionedSystemProperties(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    checkSystemPermission(credentials);
    return Optional.of(context.getPropStore().get(SystemPropKey.of()))
        .map(vProps -> new TVersionedProperties(vProps.getDataVersion(), vProps.asMap()))
        .orElseThrow();
  }

  @Override
  public Map<String,String> getTableConfiguration(TInfo tinfo, TCredentials credentials,
      String tableName) throws TException {
    TableId tableId = checkTableId(context, tableName, null);
    checkTablePermission(credentials, tableId, TablePermission.ALTER_TABLE);
    context.getPropStore().getCache().remove(TablePropKey.of(tableId));
    AccumuloConfiguration config = context.getTableConfiguration(tableId);
    return conf(credentials, config);
  }

  @Override
  public Map<String,String> getTableProperties(TInfo tinfo, TCredentials credentials,
      String tableName) throws TException {
    final TableId tableId = checkTableId(context, tableName, null);
    checkTablePermission(credentials, tableId, TablePermission.ALTER_TABLE);
    return context.getPropStore().get(TablePropKey.of(tableId)).asMap();
  }

  @Override
  public TVersionedProperties getVersionedTableProperties(TInfo tinfo, TCredentials credentials,
      String tableName) throws TException {
    final TableId tableId = checkTableId(context, tableName, null);
    checkTablePermission(credentials, tableId, TablePermission.ALTER_TABLE);
    return Optional.of(context.getPropStore().get(TablePropKey.of(tableId)))
        .map(vProps -> new TVersionedProperties(vProps.getDataVersion(), vProps.asMap()))
        .orElseThrow();
  }

  @Override
  public boolean checkClass(TInfo tinfo, TCredentials credentials, String className,
      String interfaceMatch) throws TException {
    security.authenticateUser(credentials, credentials);

    ClassLoader loader = getClass().getClassLoader();
    Class<?> shouldMatch;
    try {
      shouldMatch = loader.loadClass(interfaceMatch);
      Class<?> test = ClassLoaderUtil.loadClass(className, shouldMatch);
      test.getDeclaredConstructor().newInstance();
      return true;
    } catch (ClassCastException | ReflectiveOperationException e) {
      log.warn("Error checking object types", e);
      return false;
    }
  }

  @Override
  public boolean checkTableClass(TInfo tinfo, TCredentials credentials, String tableName,
      String className, String interfaceMatch)
      throws TException, ThriftTableOperationException, ThriftSecurityException {

    security.authenticateUser(credentials, credentials);

    TableId tableId = checkTableId(context, tableName, null);

    ClassLoader loader = getClass().getClassLoader();
    Class<?> shouldMatch;
    try {
      shouldMatch = loader.loadClass(interfaceMatch);
      AccumuloConfiguration conf = context.getTableConfiguration(tableId);
      String context = ClassLoaderUtil.tableContext(conf);
      Class<?> test = ClassLoaderUtil.loadClass(context, className, shouldMatch);
      test.getDeclaredConstructor().newInstance();
      return true;
    } catch (Exception e) {
      log.warn("Error checking object types", e);
      return false;
    }
  }

  @Override
  public boolean checkNamespaceClass(TInfo tinfo, TCredentials credentials, String ns,
      String className, String interfaceMatch)
      throws TException, ThriftTableOperationException, ThriftSecurityException {

    security.authenticateUser(credentials, credentials);

    NamespaceId namespaceId = checkNamespaceId(context, ns, null);

    ClassLoader loader = getClass().getClassLoader();
    Class<?> shouldMatch;
    try {
      shouldMatch = loader.loadClass(interfaceMatch);
      AccumuloConfiguration conf = context.getNamespaceConfiguration(namespaceId);
      String context = ClassLoaderUtil.tableContext(conf);
      Class<?> test = ClassLoaderUtil.loadClass(context, className, shouldMatch);
      test.getDeclaredConstructor().newInstance();
      return true;
    } catch (Exception e) {
      log.warn("Error checking object types", e);
      return false;
    }
  }

  @Override
  public List<TDiskUsage> getDiskUsage(Set<String> tables, TCredentials credentials)
      throws ThriftTableOperationException, ThriftSecurityException, TException {
    try {
      HashSet<TableId> tableIds = new HashSet<>();

      for (String table : tables) {
        // ensure that table table exists
        TableId tableId = checkTableId(context, table, null);
        tableIds.add(tableId);
        NamespaceId namespaceId = context.getNamespaceId(tableId);
        if (!security.canScan(credentials, tableId, namespaceId)) {
          throw new ThriftSecurityException(credentials.getPrincipal(),
              SecurityErrorCode.PERMISSION_DENIED);
        }
      }

      // use the same set of tableIds that were validated above to avoid race conditions
      Map<SortedSet<String>,Long> diskUsage = TableDiskUsage.getDiskUsage(tableIds, context);
      List<TDiskUsage> retUsages = new ArrayList<>();
      for (Map.Entry<SortedSet<String>,Long> usageItem : diskUsage.entrySet()) {
        retUsages.add(new TDiskUsage(new ArrayList<>(usageItem.getKey()), usageItem.getValue()));
      }
      return retUsages;

    } catch (TableNotFoundException e) {
      throw new TException(e);
    }
  }

  @Override
  public Map<String,String> getNamespaceConfiguration(TInfo tinfo, TCredentials credentials,
      String ns) throws TException {
    NamespaceId namespaceId = ClientServiceHandler.checkNamespaceId(context, ns, null);
    checkNamespacePermission(credentials, namespaceId, NamespacePermission.ALTER_NAMESPACE);
    context.getPropStore().getCache().remove(NamespacePropKey.of(namespaceId));
    AccumuloConfiguration config = context.getNamespaceConfiguration(namespaceId);
    return conf(credentials, config);
  }

  @Override
  public Map<String,String> getNamespaceProperties(TInfo tinfo, TCredentials credentials, String ns)
      throws TException {
    NamespaceId namespaceId = checkNamespaceId(context, ns, null);
    checkNamespacePermission(credentials, namespaceId, NamespacePermission.ALTER_NAMESPACE);
    return context.getPropStore().get(NamespacePropKey.of(namespaceId)).asMap();
  }

  @Override
  public TVersionedProperties getVersionedNamespaceProperties(TInfo tinfo, TCredentials credentials,
      String ns) throws TException {
    NamespaceId namespaceId = checkNamespaceId(context, ns, null);
    checkNamespacePermission(credentials, namespaceId, NamespacePermission.ALTER_NAMESPACE);
    return Optional.of(context.getPropStore().get(NamespacePropKey.of(namespaceId)))
        .map(vProps -> new TVersionedProperties(vProps.getDataVersion(), vProps.asMap()))
        .orElseThrow();
  }
}
