package org.apache.iotdb.hadoop.fileSystem;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HDFSUtils {

  private static final Logger logger = LoggerFactory.getLogger(HDFSUtils.class);

  private static final int MAX_ATTEMPTS_RECOVER_LEASE = 10;

  /**
   * wait for recover hdfs file lease: 1. Cannot obtain block length xxx 2. Could not obtain the
   * last block xxx 3. Blocklist for xxx has changed
   */
  public static boolean recoverFileLease(FileSystem fs, Path path) throws IOException {
    return recoverFileLease(fs, path, MAX_ATTEMPTS_RECOVER_LEASE);
  }

  public static boolean recoverFileLease(FileSystem fs, Path path, int maxAttemps)
      throws IOException {
    if (!(fs instanceof DistributedFileSystem)) return true;
    if (!fs.exists(path)) return true;
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    int attempNum = 1;
    boolean recovered = false;
    while (!Thread.currentThread().isInterrupted() && attempNum <= maxAttemps) {
      try {
        recovered = dfs.recoverLease(path);
        if (recovered) {
          break;
        }
        // check if closed
        boolean isFileClose = dfs.isFileClosed(path);
        if (!isFileClose && (attempNum % 10 == 0)) {
          logger.warn("file {} is not closed, retry {}.", path, attempNum);
        }
        attempNum += 1;
      } catch (Throwable e) {
        if (e.getMessage().contains("File does not exist")) {
          logger.info("file {} recover lease failed, message: {}", path, e.getMessage());
          break;
        }
        logger.info(
            "file {} recover lease failed {} times, message: {}, retrying...",
            path,
            attempNum,
            e.getMessage());
      }
      try {
        Thread.sleep(1000L);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
    return recovered;
  }
}
