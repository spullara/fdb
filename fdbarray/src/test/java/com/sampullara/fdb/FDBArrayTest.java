package com.sampullara.fdb;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

public class FDBArrayTest {

  private static FDBArray fdbArray;

  @BeforeClass
  public static void setup() {
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();
    DirectorySubspace ds = DirectoryLayer.getDefault().createOrOpen(db, Arrays.asList("testArray")).get();
    fdbArray = new FDBArray(db, ds, 512);
  }

  @After
  @Before
  public void before() {
    fdbArray.clear();
  }

  @Test
  public void testSimpleReadWrite() {
    byte[] bytes = new byte[12345];
    Arrays.fill(bytes, (byte) 1);
    fdbArray.write(10000, bytes).get();
    byte[] read = new byte[12345];
    fdbArray.read(read, 10000).get();
    assertArrayEquals(bytes, read);
  }

  @Test
  public void testRandomReadWrite() {
    Random r = new Random(1337);
    for (int i = 0; i < 1000; i++) {
      int length = r.nextInt(10000);
      byte[] bytes = new byte[length];
      r.nextBytes(bytes);
      int offset = r.nextInt(100000);
      fdbArray.write(offset, bytes).get();
      byte[] read = new byte[length];
      fdbArray.read(read, offset).get();
      assertArrayEquals("Iteration: " + i + ", " + length + ", " + offset,bytes, read);
    }
  }

}
