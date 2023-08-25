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

import org.apache.iotdb.tsfile.read.reader.TsFileInput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class HDFSInput extends InputStream implements TsFileInput {

  private static final Logger logger = LoggerFactory.getLogger(HDFSInput.class);
  private FSDataInputStream fsDataInputStream;
  private FileSystem fs;
  private Path path;
  private long fileSize;
  private HDFSPositionBuffer positionBuffer;

  public HDFSInput(String filePath) throws IOException {
    this(new Path(filePath), HDFSConfUtil.defaultConf);
  }

  public HDFSInput(String filePath, Configuration configuration) throws IOException {
    this(new Path(filePath), configuration);
  }

  public HDFSInput(Path path, Configuration configuration) throws IOException {
    this.fs = path.getFileSystem(configuration);
    this.path = path;
    // this.fsDataInputStream = fs.open(path);
    this.fileSize = fs.getFileStatus(path).getLen();
    this.positionBuffer = new HDFSPositionBuffer(configuration, this.fileSize);
  }

  @Override
  public long size() throws IOException {
    return fs.getFileStatus(path).getLen();
  }

  @Override
  public long position() throws IOException {
    initFsDataInputStream();
    return fsDataInputStream.getPos();
  }

  @Override
  public synchronized TsFileInput position(long newPosition) throws IOException {
    initFsDataInputStream();
    fsDataInputStream.seek(newPosition);
    return this;
  }

  @Override
  public synchronized int read(ByteBuffer dst) throws IOException {
    initFsDataInputStream();
    int res;
    if (fs instanceof ChecksumFileSystem) {
      byte[] bytes = new byte[dst.remaining()];
      res = fsDataInputStream.read(bytes);
      dst.put(bytes);
    } else {
      byte[] bytes = new byte[dst.remaining()];
      fsDataInputStream.readFully(bytes);
      res = bytes.length;
      dst.put(bytes);
    }
    return res;
  }

  @Override
  public synchronized int read(ByteBuffer dst, long position) throws IOException {
    if (position < 0) {
      throw new IllegalArgumentException("position must be non-negative");
    }
    initFsDataInputStream();

    if (position >= fileSize) {
      return -1;
    }

    byte[] bytes = this.positionBuffer.readFully(fsDataInputStream, position, dst.remaining());
    // byte[] bytes = new byte[dst.remaining()];
    // fsDataInputStream.readFully(position, bytes);
    dst.put(bytes);

    // long srcPosition = fsDataInputStream.getPos();
    // fsDataInputStream.seek(position);
    // int res = read(dst);
    // fsDataInputStream.seek(srcPosition);

    return bytes.length;
  }

  @Override
  public InputStream wrapAsInputStream() {
    return this;
  }

  @Override
  public int read() throws IOException {
    initFsDataInputStream();
    return fsDataInputStream.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return this.read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    initFsDataInputStream();
    int n = 0;
    while (n < len) {
      int count = fsDataInputStream.read(b, off + n, len - n);
      if (count < 0) break;
      n += count;
    }
    return n;
  }

  @Override
  public long skip(long n) throws IOException {
    initFsDataInputStream();
    return fsDataInputStream.skip(n);
  }

  @Override
  public int available() throws IOException {
    initFsDataInputStream();
    return fsDataInputStream.available();
  }

  @Override
  public synchronized void mark(int readlimit) {
    initFsDataInputStream();
    fsDataInputStream.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    initFsDataInputStream();
    fsDataInputStream.reset();
  }

  @Override
  public boolean markSupported() {
    initFsDataInputStream();
    return fsDataInputStream.markSupported();
  }

  @Override
  public void close() throws IOException {
    initFsDataInputStream();
    fsDataInputStream.close();
  }

  @Override
  public String getFilePath() {
    return path.toString();
  }

  private void initFsDataInputStream() {
    try {
      if (fsDataInputStream != null) {
        return;
      }
      synchronized (this) {
        if (fsDataInputStream == null) {
          fsDataInputStream = fs.open(path);
        }
      }
    } catch (IOException e) {
      logger.error("Init file input stream failed", e);
      throw new IllegalStateException(e);
    }
  }
}
