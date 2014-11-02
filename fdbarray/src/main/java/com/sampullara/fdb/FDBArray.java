package com.sampullara.fdb;

import com.foundationdb.Database;
import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.async.ReadyFuture;
import com.foundationdb.directory.DirectorySubspace;
import com.google.common.primitives.Ints;

import java.util.Arrays;

/**
 * Block storage in FDB
 */
public class FDBArray {

  // Metadata keys
  private static final String BLOCK_SIZE_KEY = "block_size";

  // Location in the database
  private final DirectorySubspace metadata;
  private final DirectorySubspace data;
  private final Database database;
  private final DirectorySubspace ds;
  private final int blockSize;

  // Used for copies
  private final ThreadLocal<byte[]> buffer = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[blockSize];
    }
  };

  /**
   * Arrays are 0 indexed byte arrays using blockSize bytes per value.
   *
   * @param blockSize
   * @param type
   * @param database
   */
  public FDBArray(Database database, DirectorySubspace ds, int blockSize) {
    this.database = database;
    this.ds = ds;
    this.metadata = ds.createOrOpen(database, Arrays.asList("metadata")).get();
    Integer currentBlocksize = database.run(new Function<Transaction, Integer>() {
      @Override
      public Integer apply(Transaction tx) {
        byte[] key = metadata.get(BLOCK_SIZE_KEY).pack();
        byte[] currentBlockSize = tx.get(key).get();
        if (currentBlockSize != null) {
          return Ints.fromByteArray(currentBlockSize);
        } else {
          tx.set(key, Ints.toByteArray(blockSize));
          return blockSize;
        }
      }
    });
    if (currentBlocksize != null && currentBlocksize != blockSize) {
      throw new IllegalArgumentException("Array already configured with block size: " + currentBlocksize);
    }
    this.data = ds.createOrOpen(database, Arrays.asList("data")).get();
    this.blockSize = blockSize;
  }

  public Future<Void> write(long offset, byte[] write) {
    return database.runAsync(new Function<Transaction, Future<Void>>() {
      @Override
      public Future<Void> apply(Transaction tx) {
        byte[] bytes = buffer.get();
        long firstBlock = offset / blockSize;
        int length = write.length;
        int blockOffset = (int) (offset % blockSize);
        long lastBlock = (offset + length) / blockSize;
        int shift = blockSize - blockOffset;
        // Special case first block and last block
        byte[] firstBlockKey = data.get(firstBlock).pack();
        Future<Void> result;
        if (blockOffset > 0 || (blockOffset == 0 && length < blockSize)) {
          // Only need to do this if the first block is partial
          result = tx.get(firstBlockKey).flatMap(new Function<byte[], Future<Void>>() {
            @Override
            public Future<Void> apply(byte[] currentBytes) {
              if (currentBytes == null) {
                // Don't use the buffer since we need it to be initialized to 0
                currentBytes = new byte[blockSize];
              }
              int writeLength = Math.min(length, shift);
              System.arraycopy(write, 0, currentBytes, blockOffset, writeLength);
              tx.set(firstBlockKey, currentBytes);
              return ReadyFuture.DONE;
            }
          });
        } else {
          // In this case copy the full first block blindly
          result = database.runAsync(new Function<Transaction, Future<Void>>() {
            @Override
            public Future<Void> apply(Transaction tx) {
              System.arraycopy(write, 0, bytes, 0, blockSize);
              tx.set(firstBlockKey, bytes);
              return ReadyFuture.DONE;
            }
          });
        }
        if (lastBlock > firstBlock) {
          // For the blocks in the middle we can just blast values in without looking at the current bytes
          for (long i = firstBlock + 1; i < lastBlock; i++) {
            byte[] key = data.get(i).pack();
            int writeBlock = (int) (i - firstBlock);
            int position = (writeBlock - 1) * blockSize + shift;
            System.arraycopy(write, position, bytes, 0, blockSize);
            tx.set(key, bytes);
          }
          byte[] lastBlockKey = data.get(lastBlock).pack();
          result = result.flatMap($ -> tx.get(lastBlockKey).flatMap(new Function<byte[], Future<Void>>() {
            @Override
            public Future<Void> apply(byte[] bytes) {
              if (bytes == null) {
                // Don't use the buffer since we need it to be initialized to 0
                bytes = new byte[blockSize];
              }
              int position = (int) ((lastBlock - firstBlock - 1) * blockSize + shift);
              System.arraycopy(write, position, bytes, 0, length - position);
              tx.set(lastBlockKey, bytes);
              return ReadyFuture.DONE;
            }
          }));
        }
        return result;
      }
    });
  }

  public Future<Void> read(byte[] read, long offset) {
    return database.runAsync(new Function<Transaction, Future<Void>>() {
      @Override
      public Future<Void> apply(Transaction tx) {
        long firstBlock = offset / blockSize;
        int blockOffset = (int) (offset % blockSize);
        int length = read.length;
        long lastBlock = (offset + length) / blockSize;
        for (KeyValue keyValue : tx.getRange(data.get(firstBlock).pack(), data.get(lastBlock + 1).pack())) {
          long blockId = data.unpack(keyValue.getKey()).getLong(0);
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
        return ReadyFuture.DONE;
      }
    });
  }

  public void clear() {
    database.run(new Function<Transaction, Void>() {
      @Override
      public Void apply(Transaction tx) {
        tx.clear(ds.pack());
        return null;
      }
    });
  }
}
