package com.sampullara.fdb;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FDBArrayTest {

  private static FDBArray fdbArray;

  @BeforeClass
  public static void setup() {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();
    FDBArray.create(db, "testArray", 512);
    fdbArray = FDBArray.open(db, "testArray");
  }

  @AfterClass
  public static void cleanup() {
    fdbArray.delete();
  }

  @After
  @Before
  public void delete() {
    fdbArray.clear();
  }

  @Test
  public void testSimpleReadWrite() {
    byte[] bytes = new byte[12345];
    Arrays.fill(bytes, (byte) 1);
    fdbArray.write(bytes, 10000).get();
    byte[] read = new byte[12345];
    fdbArray.read(read, 10000).get();
    assertArrayEquals(bytes, read);
    assertEquals((12345 / 512 + 1) * 512, fdbArray.usage().get().longValue());
  }

  @Test
  public void testReadOnly() {
    byte[] bytes = new byte[12345];
    Arrays.fill(bytes, (byte) 1);
    fdbArray.write(bytes, 10000).get();
    FDBArray snapshot = fdbArray.snapshot();

    byte[] read = new byte[12345];
    snapshot.read(read, 10000).get();
    assertArrayEquals(bytes, read);

    try {
      snapshot.write(bytes, 10000).get();
      fail("Should be read only");
    } catch (IllegalStateException ise) {
      // Read only
    }
  }

  @Test
  public void testSnapshots() throws InterruptedException {
    Random r = new Random(1337);
    byte[] bytes = new byte[12345];
    r.nextBytes(bytes);
    fdbArray.write(bytes, 10000).get();
    byte[] read = new byte[12345];
    fdbArray.read(read, 10000).get();
    assertArrayEquals(bytes, read);
    long timestamp = System.currentTimeMillis();
    Thread.sleep(10);
    byte[] nextBytes = new byte[12345];
    r.nextBytes(nextBytes);
    fdbArray.write(nextBytes, 10000).get();
    fdbArray.read(read, 10000);
    assertArrayEquals(nextBytes, read);
    fdbArray.read(read, 10000, timestamp);
    assertArrayEquals(bytes, read);

    byte[] empty = new byte[12345];
    byte[] readEmpty = new byte[12345];
    fdbArray.read(readEmpty, 10000, 0).get();
    assertArrayEquals(readEmpty, empty);
  }

  @Test
  public void testParent() {
    Random r = new Random(1337);
    byte[] parentBytes = new byte[1000];
    r.nextBytes(parentBytes);
    fdbArray.write(parentBytes, 1000).get();
    byte[] parentRead = new byte[1000];
    fdbArray.read(parentRead, 1000).get();
    assertArrayEquals(parentBytes, parentRead);

    // Should start with a snapshot of the parent, need to delete first for testing
    FDBArray fdbChildArray = fdbArray.snapshot("testChildArray");
    try {
      byte[] childRead = new byte[1000];
      fdbChildArray.read(childRead, 1000).get();
      assertArrayEquals(parentBytes, childRead);

      byte[] childBytes = new byte[1000];
      r.nextBytes(childBytes);
      fdbChildArray.write(childBytes, 1500).get();

      byte[] mixedRead = new byte[1500];
      fdbChildArray.read(mixedRead, 1000).get();

      for (int i = 0; i < 500; i++) {
        assertEquals("Failed: " + i, parentBytes[i], mixedRead[i]);
      }
      for (int i = 500; i < 1500; i++) {
        assertEquals("Failed: " + i, childBytes[i - 500], mixedRead[i]);
      }
    } finally {
      fdbChildArray.delete();
    }
  }

  @Test
  public void testRandomReadWrite() {
    Random r = new Random(1337);
    for (int i = 0; i < 1000; i++) {
      int length = r.nextInt(10000);
      byte[] bytes = new byte[length];
      r.nextBytes(bytes);
      int offset = r.nextInt(100000);
      fdbArray.write(bytes, offset).get();
      byte[] read = new byte[length];
      fdbArray.read(read, offset).get();
      assertArrayEquals("Iteration: " + i + ", " + length + ", " + offset, bytes, read);
    }
    assertEquals((110000 / 512 + 1) * 512, fdbArray.usage().get().longValue());
  }
}
