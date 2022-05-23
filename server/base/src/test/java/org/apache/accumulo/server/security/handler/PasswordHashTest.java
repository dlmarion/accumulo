package org.apache.accumulo.server.security.handler;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.UUID;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.apache.commons.codec.digest.Crypt;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class PasswordHashTest {
  
  private static final int SALT_LENGTH = 8;
  private static final SecureRandom RANDOM = new SecureRandom();
  private static final int NUM_PASSWORDS = 1000;
  
  // Generates a byte array salt of length SALT_LENGTH
  private static byte[][] generateSalts(int numPasswords) {
    byte[][] salts = new byte[numPasswords][];
    for (int i = 0; i < numPasswords; i++) {
      byte[] salt = new byte[SALT_LENGTH];
      RANDOM.nextBytes(salt);
      salts[i] = salt;     
    }
    return salts;
  }
  
  private static byte[][] generatePasswords(int numPasswords) {
    byte[][] passwords = new byte[numPasswords][];
    for (int i = 0; i < numPasswords; i++) {
      passwords[i] = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    }   
    return passwords;
  }

  private static byte[][] salts;
  private static byte[][] passwords;    
  
  @BeforeAll
  public static void before() throws Exception {
    salts = generateSalts(NUM_PASSWORDS);
    passwords = generatePasswords(NUM_PASSWORDS);    
  }
  
  @Test
  public void testCrypt() throws Exception {
    for (int x=0; x < NUM_PASSWORDS; x++) {
      String passHash = Crypt.crypt(passwords[x]);
      assertNotNull(passHash);
    }
  }

  @Test
  public void testPBKDF2WithHmacSHA512() throws Exception {
    SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
    int iterations = 1000;
    
    for (int x=0; x < NUM_PASSWORDS; x++) {
      PBEKeySpec spec = new PBEKeySpec(new String(passwords[x], StandardCharsets.UTF_8).toCharArray(),
          salts[x], iterations, 64 * 8);
      byte[] hash = skf.generateSecret(spec).getEncoded();
      assertNotNull(hash);
    }
  }
}
