/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.hadoop.fileSystem;

import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * This class is used to wrap the {@link}FSDataOutputStream and implement the interface
 * {@link}TsFileOutput
 */
public class HDFSOutput implements TsFileOutput {
  private static final Logger logger = LoggerFactory.getLogger(HDFSOutput.class);

  private FSDataOutputStream fsDataOutputStream;
  private FileSystem fs;
  private Path path;

  public HDFSOutput(String filePath, boolean overwrite) throws IOException {
    this(filePath, HDFSConfUtil.defaultConf, overwrite);
    path = new Path(filePath);
  }

  public HDFSOutput(String filePath, Configuration configuration, boolean overwrite)
      throws IOException {
    this(new Path(filePath), configuration, overwrite);
    path = new Path(filePath);
  }

  public HDFSOutput(Path path, Configuration configuration, boolean overwrite) throws IOException {
    fs = path.getFileSystem(HDFSConfUtil.setConf(configuration));
    if (fs.exists(path)) {
      boolean recovered = HDFSUtils.recoverFileLease(fs, path, 10);
      if (!recovered) {
        Path recoverPath = new Path(path.getParent(), path.getName() + ".recover");
        recoverFileByCopy(path, recoverPath, Long.MAX_VALUE);
      }
      fsDataOutputStream = fs.append(path);
    } else {
      fsDataOutputStream = fs.create(path, overwrite);
    }
    this.path = path;
  }

  @Override
  public void write(byte[] b) throws IOException {
    fsDataOutputStream.write(b);
  }

  @Override
  public void write(byte b) throws IOException {
    fsDataOutputStream.write(b);
  }

  @Override
  public void write(ByteBuffer b) throws IOException {
    fsDataOutputStream.write(b.array()); // direct memory ?
  }

  @Override
  public long getPosition() throws IOException {
    return fsDataOutputStream.getPos();
  }

  @Override
  public void close() throws IOException {
    fsDataOutputStream.close();
  }

  @Override
  public OutputStream wrapAsStream() {
    return fsDataOutputStream;
  }

  @Override
  public void flush() throws IOException {
    this.fsDataOutputStream.hflush();
  }

  @Override
  public void truncate(long size) throws IOException {
    if (fs.exists(path)) {
      try {
        fsDataOutputStream.close();
      } catch (Throwable e) {
        // ignore
      }
    }
    boolean truncated = false;
    try {
      truncated = fs.truncate(path, size);
    } catch (Throwable e) {
      logger.warn("Unkonwn error when truncate", e);
    }
    if (!truncated) {
      // clients should wait for it to complete before proceeding with further file updates.
      // boolean recovered = HDFSUtils.recoverFileLease(fs, path, 100);
      boolean recovered = HDFSUtils.recoverFileLease(fs, path, 10);
      if (!recovered) {
        Path recoverPath = new Path(path.getParent(), path.getName() + ".recover");
        recoverFileByCopy(path, recoverPath, size);
      }
    }
    if (fs.exists(path)) {
      fsDataOutputStream = fs.append(path);
    }
  }

  private void recoverFileByCopy(Path src, Path dest, long size) throws IOException {
    try (FSDataInputStream inputStream = fs.open(src);
        FSDataOutputStream outputStream = fs.create(dest)) {
      long count = 0;
      int n;
      byte[] buffer = new byte[8192];
      while (count < size && -1 != (n = inputStream.read(buffer))) {
        count += n;
        if (count > size) {
          outputStream.write(buffer, 0, (int) (size + n - count));
        } else {
          outputStream.write(buffer, 0, n);
        }
      }
    } catch (Throwable e) {
      logger.warn("Unkonwn error", e);
      throw e;
    }
    logger.warn(
        "Use copy to recover file with truncate size {} from {}[{}] to {}[{}]",
        size,
        src,
        fs.getFileStatus(src).getLen(),
        dest,
        fs.getFileStatus(dest).getLen());
    fs.delete(src, true);
    fs.rename(dest, src);
  }
}
