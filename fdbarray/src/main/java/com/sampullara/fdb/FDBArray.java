package com.sampullara.fdb;

import com.foundationdb.Database;
import com.foundationdb.KeyValue;
import com.foundationdb.ReadTransaction;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.async.ReadyFuture;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.tuple.Tuple;
import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.List;

/**
 * Block storage in FDB
 */
public class FDBArray {

  // Metadata keys
  private static final String BLOCK_SIZE_KEY = "block_size";
  private static final String PARENT_KEY = "parent";
  private static final String PARENT_TIMESTAMP_KEY = "parent_timestamp";

  // Location in the database
  private final DirectorySubspace metadata;
  private final DirectorySubspace data;
  private final Database database;
  private final int blockSize;
  private final FDBArray parentArray;
  private final DirectorySubspace ds;
  private final Long snapshot;

  // Used for copies
  private final ThreadLocal<byte[]> buffer = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[blockSize];
    }
  };

  public static FDBArray create(Database database, DirectorySubspace ds, int blockSize, DirectorySubspace parent, long timestamp) {
    DirectorySubspace metadata = ds.create(database, Arrays.asList("metadata")).get();
    if (parent != null) {
      List<String> parentPath = parent.getPath();
      database.run(new Function<Transaction, Void>() {
        @Override
        public Void apply(Transaction tx) {
          tx.set(metadata.get(PARENT_KEY).pack(), Tuple.fromList(parentPath).pack());
          tx.set(metadata.get(PARENT_TIMESTAMP_KEY).pack(), Tuple.from(timestamp).pack());
          return null;
        }
      });
    }
    database.run(new Function<Transaction, Void>() {
      @Override
      public Void apply(Transaction tx) {
        tx.set(metadata.get(BLOCK_SIZE_KEY).pack(), Ints.toByteArray(blockSize));
        return null;
      }
    });
    return new FDBArray(database, ds);
  }

  public FDBArray(Database database, DirectorySubspace ds, Long snapshot) {
    this.ds = ds;
    this.snapshot = snapshot;
    this.database = database;
    this.metadata = ds.createOrOpen(database, Arrays.asList("metadata")).get();
    this.data = ds.createOrOpen(database, Arrays.asList("data")).get();
    Integer currentBlocksize = database.run(new Function<Transaction, Integer>() {
      @Override
      public Integer apply(Transaction tx) {
        byte[] key = metadata.get(BLOCK_SIZE_KEY).pack();
        byte[] currentBlockSize = tx.get(key).get();
        if (currentBlockSize == null) {
          return null;
        } else {
          return Ints.fromByteArray(currentBlockSize);
        }
      }
    });
    if (currentBlocksize == null) {
      throw new IllegalArgumentException("Block size for array not configured");
    } else {
      blockSize = currentBlocksize;
    }
    parentArray = database.run(new Function<Transaction, FDBArray>() {
      @Override
      public FDBArray apply(Transaction tx) {
        byte[] parentPathValue = tx.get(metadata.get(PARENT_KEY).pack()).get();
        byte[] parentTimestampBytes = tx.get(metadata.get(PARENT_TIMESTAMP_KEY).pack()).get();
        if (parentPathValue == null) {
          return null;
        } else {
          List<String> items = (List) Tuple.fromBytes(parentPathValue).getItems();
          long parentTimestamp = Tuple.fromBytes(parentTimestampBytes).getLong(0);
          return new FDBArray(database, DirectoryLayer.getDefault().open(database, items).get(), parentTimestamp);
        }
      }
    });
  }

  /**
   * Arrays are 0 indexed byte arrays using blockSize bytes per value.
   *
   * @param blockSize
   * @param type
   * @param database
   */
  public FDBArray(Database database, DirectorySubspace ds) {
    this(database, ds, null);
  }

  public Future<Void> write(byte[] write, long offset) {
    if (snapshot != null) {
      throw new IllegalStateException("FDBArray is read only");
    }
    return database.runAsync(new Function<Transaction, Future<Void>>() {
      @Override
      public Future<Void> apply(Transaction tx) {
        // Use a single buffer for all full blocksize writes
        byte[] bytes = buffer.get();

        // Calculate the block locations
        int length = write.length;
        long firstBlock = offset / blockSize;
        long lastBlock = (offset + length) / blockSize;
        int blockOffset = (int) (offset % blockSize);
        int shift = blockSize - blockOffset;

        // Special case first block and last block
        byte[] firstBlockKey = data.get(firstBlock).get(System.currentTimeMillis()).pack();
        if (blockOffset > 0 || (blockOffset == 0 && length < blockSize)) {
          // Only need to do this if the first block is partial
          byte[] readBytes = new byte[blockSize];
          read(tx, firstBlock * blockSize, readBytes, Long.MAX_VALUE);
          int writeLength = Math.min(length, shift);
          System.arraycopy(write, 0, readBytes, blockOffset, writeLength);
          tx.set(firstBlockKey, readBytes);
        } else {
          // In this case copy the full first block blindly
          System.arraycopy(write, 0, bytes, 0, blockSize);
          tx.set(firstBlockKey, bytes);
        }
        // If there is more than one block
        if (lastBlock > firstBlock) {
          // For the blocks in the middle we can just blast values in without looking at the current bytes
          for (long i = firstBlock + 1; i < lastBlock; i++) {
            byte[] key = data.get(i).get(System.currentTimeMillis()).pack();
            int writeBlock = (int) (i - firstBlock);
            int position = (writeBlock - 1) * blockSize + shift;
            System.arraycopy(write, position, bytes, 0, blockSize);
            tx.set(key, bytes);
          }
          int position = (int) ((lastBlock - firstBlock - 1) * blockSize + shift);
          int lastBlockLength = length - position;
          byte[] lastBlockKey = data.get(lastBlock).get(System.currentTimeMillis()).pack();
          // If the last block is a complete block we don't need to read
          if (lastBlockLength == blockSize) {
            System.arraycopy(write, position, bytes, 0, blockSize);
            tx.set(lastBlockKey, bytes);
          } else {
            byte[] readBytes = new byte[blockSize];
            read(tx, lastBlock * blockSize, readBytes, Long.MAX_VALUE);
            System.arraycopy(write, position, readBytes, 0, lastBlockLength);
            tx.set(lastBlockKey, readBytes);
          }
        }
        return ReadyFuture.DONE;
      }
    });
  }

  /**
   * Read latest blocks.
   *
   * @param read
   * @param offset
   * @return
   */
  public Future<Void> read(byte[] read, long offset) {
    return read(read, offset, Long.MAX_VALUE);
  }

  /**
   * Read blocks as of a particular timestamp.
   *
   * @param read
   * @param offset
   * @param timestamp
   * @return
   */
  public Future<Void> read(byte[] read, long offset, long timestamp) {
    return database.runAsync(new Function<Transaction, Future<Void>>() {
      @Override
      public Future<Void> apply(Transaction tx) {
        read(tx, offset, read, timestamp);
        return ReadyFuture.DONE;
      }
    });
  }

  private void read(ReadTransaction tx, long offset, byte[] read, long readTimestamp) {
    long snapshotTimestamp = snapshot == null ? readTimestamp : Math.min(readTimestamp, snapshot);
    if (parentArray != null) parentArray.read(tx, offset, read, snapshotTimestamp);
    long firstBlock = offset / blockSize;
    int blockOffset = (int) (offset % blockSize);
    int length = read.length;
    long lastBlock = (offset + length) / blockSize;
    for (KeyValue keyValue : tx.getRange(data.get(firstBlock).pack(), data.get(lastBlock + 1).pack())) {
      Tuple keyTuple = data.unpack(keyValue.getKey());
      long blockId = keyTuple.getLong(0);
      long timestamp = keyTuple.getLong(1);
      if (timestamp <= snapshotTimestamp) {
        // This is a super naive way to do this. Basically we always copy
        // the block into the output unless it is too new. We will then
        // end up with the latest value in the output. However, we should
        // only copy the latest value rather than do this extra work.
        byte[] value = keyValue.getValue();
        int blockPosition = (int) ((blockId - firstBlock) * blockSize);
        int shift = blockSize - blockOffset;
        if (blockId == firstBlock) {
          int firstBlockLength = Math.min(shift, read.length);
          System.arraycopy(value, blockOffset, read, 0, firstBlockLength);
        } else {
          int position = blockPosition - blockSize + shift;
          if (blockId == lastBlock) {
            int lastLength = read.length - position;
            System.arraycopy(value, 0, read, position, lastLength);
          } else {
            System.arraycopy(value, 0, read, position, blockSize);
          }
        }
      }
    }
  }

  public FDBArray snapshot() {
    return new FDBArray(database, ds, System.currentTimeMillis());
  }

  public void clear() {
    database.run(new Function<Transaction, Void>() {
      @Override
      public Void apply(Transaction tx) {
        tx.clear(data.pack());
        return null;
      }
    });
  }

  public void setMetadata(byte[] key, byte[] value) {
    database.run(new Function<Transaction, Object>() {
      @Override
      public Object apply(Transaction tx) {
        tx.set(metadata.get(key).pack(), value);
        return null;
      }
    });
  }

  public byte[] getMetadata(byte[] key) {
    return database.run(new Function<Transaction, byte[]>() {
      @Override
      public byte[] apply(Transaction tx) {
        return tx.get(key).get();
      }
    });
  }
}
