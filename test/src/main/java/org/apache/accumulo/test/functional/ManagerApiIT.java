package org.apache.accumulo.test.functional;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.ClientExec;
import org.apache.accumulo.core.client.impl.ClientExecReturn;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterClientService.Client;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MiniClusterOnlyTests.class)
public class ManagerApiIT extends AccumuloClusterHarness {
  
  private static final Logger LOG = LoggerFactory.getLogger(ManagerApiIT.class);
  
  private interface MasterApiMethodTest {
    public void test() throws Exception;
  }
  
  private class InitiateFlush implements MasterApiMethodTest {
    @Override
    public void test() throws Exception {
      MasterClient.execute(getContext(), new ClientExecReturn<Long,Client>() {
        @Override
        public Long execute(Client client) throws Exception {
          String tableId = getContext().getConnector().tableOperations().tableIdMap().get("ns1.NO_SUCH_TABLE");
          return client.initiateFlush(null, getContext().rpcCreds(), tableId);
        }
      });
    }
  }
  
  private class ShutDownTServer implements MasterApiMethodTest {
    public void test() throws Exception {
      MasterClient.executeVoid(getContext(), new ClientExec<MasterClientService.Client>() {
        @Override
        public void execute(MasterClientService.Client client) throws Exception {
          LOG.info("Sending shutdown command to {} via MasterClientService", "NO_SUCH_TSERVER");
          client.shutdownTabletServer(null, getContext().rpcCreds(), "NO_SUCH_TSERVER", false);
        }
      });      
    }
  }

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }
  
  private ClientContext getContext() {
    return new ClientContext(this.getConnector().getInstance(), user, getClusterConfiguration().getClientConf());
  }

  private void runTest(MasterApiMethodTest test, boolean expectException, boolean expectPermissionDenied) throws Exception {
    try {
      test.test();
    } catch (Exception ex) {
      if (!expectException) {
        throw ex;
      }
      if (ex instanceof AccumuloSecurityException && 
           ((AccumuloSecurityException) ex).getSecurityErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED)) {
        if (!expectPermissionDenied) {
          throw ex;
        }
      }
    }
  }
  
  private volatile Credentials user = null;
  
  @Test
  public void testMasterApi() throws Exception {
    // Set user to ADMIN user
    Credentials admin = new Credentials(getAdminPrincipal(), getAdminToken());
    Credentials user1 = new Credentials("user1", new PasswordToken("user1"));
    Credentials user2 = new Credentials("user2", new PasswordToken("user2"));
    
    user = admin;
    getContext().getConnector().securityOperations().createLocalUser(user1.getPrincipal(), (PasswordToken) user1.getToken());
    getContext().getConnector().securityOperations().createLocalUser(user2.getPrincipal(), (PasswordToken) user2.getToken());
    getContext().getConnector().securityOperations().grantSystemPermission(user1.getPrincipal(), SystemPermission.CREATE_NAMESPACE);
    
    user = user1;
    getContext().getConnector().namespaceOperations().create("ns1");
    getContext().getConnector().tableOperations().create("ns1.NO_SUCH_TABLE");
    getContext().getConnector().securityOperations().grantTablePermission(user2.getPrincipal(), "ns1.NO_SUCH_TABLE", TablePermission.WRITE);
    
    // To Flush, user needs WRITE or ALTER TABLE
    user = admin;
    runTest(new InitiateFlush(), true, true); // admin has no privs on table
    user = user1;
    runTest(new InitiateFlush(), false, false); // user1 created the table
    user = user2;
    runTest(new InitiateFlush(), true, false); // user2 has write permissions

    user = admin;
    runTest(new ShutDownTServer(), true, false); // admin has no privs on table
    user = user1;
    runTest(new ShutDownTServer(), true, true); // user1 created the table
    user = user2;
    runTest(new ShutDownTServer(), true, true); // user2 has write permissions

  }
  
}
