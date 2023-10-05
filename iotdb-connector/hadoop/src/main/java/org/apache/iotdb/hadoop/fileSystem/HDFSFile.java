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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HDFSFile extends File {

  private static final long serialVersionUID = -8419827359081949547L;
  private transient Path hdfsPath;
  private transient FileSystem fs;
  private static final Logger logger = LoggerFactory.getLogger(HDFSFile.class);
  private static final String UNSUPPORT_OPERATION = "Unsupported operation.";

  public HDFSFile(String pathname) {
    super(pathname);
    hdfsPath = new Path(pathname);
    setConfAndGetFS();
  }

  public HDFSFile(String parent, String child) {
    super(parent, child);
    hdfsPath = new Path(parent + File.separator + child);
    setConfAndGetFS();
  }

  public HDFSFile(File parent, String child) {
    super(parent, child);
    hdfsPath = new Path(parent.getAbsolutePath() + File.separator + child);
    setConfAndGetFS();
  }

  public HDFSFile(URI uri) {
    super(uri.toString());
    hdfsPath = new Path(uri);
    setConfAndGetFS();
  }

  private void setConfAndGetFS() {
    try {
      fs = HDFSConfUtil.defaultFs;
      if (fs == null) {
        fs = hdfsPath.getFileSystem(HDFSConfUtil.defaultConf);
      }
    } catch (IOException e) {
      logger.error("Fail to get HDFS! ", e);
    }
  }

  @Override
  public String getAbsolutePath() {
    return hdfsPath.toUri().toString();
  }

  @Override
  public String getPath() {
    return hdfsPath.toUri().toString();
  }

  @Override
  public long length() {
    try {
      return fs.getFileStatus(hdfsPath).getLen();
    } catch (IOException e) {
      logger.error("Fail to get length of the file {}, ", hdfsPath.toUri(), e);
      return 0;
    }
  }

  @Override
  public boolean exists() {
    try {
      return fs.exists(hdfsPath);
    } catch (IOException e) {
      logger.error("Fail to check whether the file {} exists. ", hdfsPath.toUri(), e);
      return false;
    }
  }

  @Override
  public @Nullable File[] listFiles() {
    List<HDFSFile> files = new ArrayList<>();
    try {
      for (FileStatus fileStatus : fs.listStatus(hdfsPath)) {
        Path filePath = fileStatus.getPath();
        files.add(new HDFSFile(filePath.toUri().toString()));
      }
      return files.toArray(new HDFSFile[0]);
    } catch (IOException e) {
      logger.error("Fail to list files in {}. ", hdfsPath.toUri(), e);
      return null;
    }
  }

  @Override
  public File getParentFile() {
    return new HDFSFile(hdfsPath.getParent().toUri().toString());
  }

  @Override
  public boolean createNewFile() throws IOException {
    return fs.createNewFile(hdfsPath);
  }

  @Override
  public boolean delete() {
    try {
      return !fs.exists(hdfsPath) || fs.delete(hdfsPath, true);
    } catch (IOException e) {
      logger.error("Fail to delete file {}. ", hdfsPath.toUri(), e);
      return false;
    }
  }

  @Override
  public boolean mkdirs() {
    try {
      return !exists() && fs.mkdirs(hdfsPath);
    } catch (IOException e) {
      logger.error("Fail to create directory {}. ", hdfsPath.toUri(), e);
      return false;
    }
  }

  @Override
  public boolean isDirectory() {
    try {
      return exists() && fs.getFileStatus(hdfsPath).isDirectory();
    } catch (IOException e) {
      logger.error("Fail to judge whether {} is a directory. ", hdfsPath.toUri(), e);
      return false;
    }
  }

  @Override
  public long getFreeSpace() {
    try {
      return fs.getStatus().getRemaining();
    } catch (IOException e) {
      logger.error("Fail to get free space of {}. ", hdfsPath.toUri(), e);
      return 0L;
    }
  }

  @Override
  public String getName() {
    return hdfsPath.getName();
  }

  @Override
  public String toString() {
    return hdfsPath.toUri().toString();
  }

  @Override
  public int hashCode() {
    return hdfsPath.hashCode();
  }

  @Override
  public int compareTo(File pathname) {
    if (pathname instanceof HDFSFile) {
      return hdfsPath.toUri().toString().compareTo(pathname.getPath());
    } else {
      logger.error("File {} is not HDFS file. ", pathname.getPath());
      throw new IllegalArgumentException("Compare file is not HDFS file.");
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof HDFSFile && compareTo((HDFSFile) obj) == 0;
  }

  @Override
  public boolean renameTo(File dest) {
    try {
      return fs.rename(hdfsPath, new Path(dest.getAbsolutePath()));
    } catch (IOException e) {
      logger.error("Failed to rename file {} to {}. ", hdfsPath, dest.getName(), e);
      return false;
    }
  }

  public BufferedReader getBufferedReader(String filePath) {
    try {
      Path p = new Path(filePath);
      HDFSUtils.recoverFileLease(fs, p);
      return new BufferedReader(new InputStreamReader(fs.open(p)));
    } catch (IOException e) {
      logger.error("Failed to get buffered reader for {}. ", filePath, e);
      return null;
    }
  }

  public BufferedWriter getBufferedWriter(String filePath) {
    try {
      Path p = new Path(filePath);
      HDFSUtils.recoverFileLease(fs, p);
      return new BufferedWriter(new OutputStreamWriter(fs.create(p)));
    } catch (IOException e) {
      logger.error("Failed to get buffered writer for {}. ", filePath, e);
      return null;
    }
  }

  public BufferedWriter getBufferedWriter(String filePath, boolean append) {
    try {
      Path p = new Path(filePath);
      if (append) {
        if (!fs.exists(p)) {
          fs.createNewFile(p);
        }
        HDFSUtils.recoverFileLease(fs, p);
        return new BufferedWriter(new OutputStreamWriter(fs.append(p)));
      }
      // else create file
      HDFSUtils.recoverFileLease(fs, p);
      return new BufferedWriter(new OutputStreamWriter(fs.create(p)));
    } catch (IOException e) {
      logger.error("Failed to get buffered writer for {}. ", filePath, e);
      return null;
    }
  }

  public BufferedInputStream getBufferedInputStream(String filePath) {
    try {
      Path p = new Path(filePath);
      HDFSUtils.recoverFileLease(fs, p);
      return new BufferedInputStream(fs.open(p));
    } catch (IOException e) {
      logger.error("Failed to get buffered input stream for {}. ", filePath, e);
      return null;
    }
  }

  public BufferedOutputStream getBufferedOutputStream(String filePath) {
    try {
      Path p = new Path(filePath);
      HDFSUtils.recoverFileLease(fs, p);
      return new BufferedOutputStream(fs.create(p));
    } catch (IOException e) {
      logger.error("Failed to get buffered output stream for {}. ", filePath, e);
      return null;
    }
  }

  public File[] listFilesBySuffix(String fileFolder, String suffix) {
    PathFilter pathFilter = path -> path.toUri().toString().endsWith(suffix);
    List<HDFSFile> files = listFiles(fileFolder, pathFilter);
    return files.toArray(new HDFSFile[0]);
  }

  public File[] listFilesByPrefix(String fileFolder, String prefix) {
    PathFilter pathFilter = path -> path.toUri().toString().startsWith(prefix);
    List<HDFSFile> files = listFiles(fileFolder, pathFilter);
    return files.toArray(new HDFSFile[0]);
  }

  private List<HDFSFile> listFiles(String fileFolder, PathFilter pathFilter) {
    List<HDFSFile> files = new ArrayList<>();
    try {
      Path path = new Path(fileFolder);
      for (FileStatus fileStatus : fs.listStatus(path)) {
        Path filePath = fileStatus.getPath();
        if (pathFilter.accept(filePath)) {
          HDFSFile file = new HDFSFile(filePath.toUri().toString());
          files.add(file);
        }
      }
    } catch (IOException e) {
      logger.error("Failed to list files in {}. ", fileFolder);
    }
    return files;
  }

  public void copyToLocal(File destFile) throws IOException {
    fs.copyToLocalFile(hdfsPath, new Path(destFile.getPath()));
  }

  public void copyFromLocal(File srcFile) throws IOException {
    fs.copyFromLocalFile(new Path(srcFile.getPath()), hdfsPath);
  }

  public void copyTo(File destFile) throws IOException {
    try (InputStream in = fs.open(hdfsPath);
        OutputStream out = fs.create(((HDFSFile) destFile).hdfsPath, true)) {
      IOUtils.copyBytes(in, out, 4096);
    }
  }

  @Override
  public File getAbsoluteFile() {
    return new HDFSFile(getAbsolutePath());
  }

  @Override
  public String getParent() {
    return hdfsPath.getParent().toString();
  }

  @Override
  public boolean isAbsolute() {
    return hdfsPath.isAbsolute();
  }

  @Override
  public File[] listFiles(FileFilter filter) {
    try {
      return Arrays.stream(
              fs.listStatus(hdfsPath, path -> filter.accept(new HDFSFile(path.toUri()))))
          .map(f -> new HDFSFile(f.getPath().toUri()))
          .toArray(File[]::new);
    } catch (IOException e) {
      logger.error("Fail to list of {}. ", hdfsPath.toUri(), e);
      return null;
    }
  }

  @Override
  public String getCanonicalPath() {
    return hdfsPath.toString();
  }

  @Override
  public File getCanonicalFile() {
    return this;
  }

  @Override
  public URL toURL() {
    try {
      return hdfsPath.toUri().toURL();
    } catch (IOException e) {
      logger.error("Fail to get URL of {}. ", hdfsPath.toUri(), e);
      return null;
    }
  }

  @Override
  public URI toURI() {
    return hdfsPath.toUri();
  }

  @Override
  public boolean canRead() {
    return true;
  }

  @Override
  public boolean canWrite() {
    return true;
  }

  @Override
  public boolean isFile() {
    try {
      return fs.getFileStatus(hdfsPath).isFile();
    } catch (IOException e) {
      logger.error("Fail to get is file of {}. ", hdfsPath.toUri(), e);
      return false;
    }
  }

  @Override
  public boolean isHidden() {
    return hdfsPath.getName().startsWith(".");
  }

  @Override
  public long lastModified() {
    try {
      return fs.getFileStatus(hdfsPath).getModificationTime();
    } catch (IOException e) {
      logger.error("Fail to delete on exit of {}. ", hdfsPath.toUri(), e);
      return 0L;
    }
  }

  @Override
  public void deleteOnExit() {
    try {
      fs.deleteOnExit(hdfsPath);
    } catch (IOException e) {
      logger.error("Fail to delete on exit of {}. ", hdfsPath.toUri(), e);
    }
  }

  @Override
  public String[] list() {
    try {
      return Arrays.stream(fs.listStatus(hdfsPath))
          .map(f -> f.getPath().toString())
          .toArray(String[]::new);
    } catch (IOException e) {
      logger.error("Fail to list of {}. ", hdfsPath.toUri(), e);
      return null;
    }
  }

  @Override
  public String[] list(FilenameFilter filter) {
    try {
      return Arrays.stream(
              fs.listStatus(
                  hdfsPath, path -> filter.accept(new HDFSFile(path.toUri()), path.getName())))
          .map(f -> f.getPath().toString())
          .toArray(String[]::new);
    } catch (IOException e) {
      logger.error("Fail to list of {}. ", hdfsPath.toUri(), e);
      return null;
    }
  }

  @Override
  public File[] listFiles(FilenameFilter filter) {
    try {
      return Arrays.stream(
              fs.listStatus(
                  hdfsPath, path -> filter.accept(new HDFSFile(path.toUri()), path.getName())))
          .map(f -> new HDFSFile(f.getPath().toUri()))
          .toArray(File[]::new);
    } catch (IOException e) {
      logger.error("Fail to listFiles of {}. ", hdfsPath.toUri(), e);
      return null;
    }
  }

  @Override
  public boolean mkdir() {
    try {
      return fs.mkdirs(hdfsPath);
    } catch (IOException e) {
      logger.error("Fail to mkdir of {}. ", hdfsPath.toUri(), e);
      return false;
    }
  }

  @Override
  public boolean setLastModified(long time) {
    try {
      fs.setTimes(hdfsPath, time, fs.getFileStatus(hdfsPath).getAccessTime());
      return true;
    } catch (IOException e) {
      logger.error("Fail to set last modified of {}. ", hdfsPath.toUri(), e);
      return false;
    }
  }

  @Override
  public boolean setReadOnly() {
    return false;
  }

  @Override
  public boolean setWritable(boolean writable, boolean ownerOnly) {
    return false;
  }

  @Override
  public boolean setWritable(boolean writable) {
    return false;
  }

  @Override
  public boolean setReadable(boolean readable, boolean ownerOnly) {
    return false;
  }

  @Override
  public boolean setReadable(boolean readable) {
    return false;
  }

  @Override
  public boolean setExecutable(boolean executable, boolean ownerOnly) {
    return false;
  }

  @Override
  public boolean setExecutable(boolean executable) {
    return false;
  }

  @Override
  public boolean canExecute() {
    try {
      return fs.getFileStatus(hdfsPath).getPermission().getUserAction().implies(FsAction.EXECUTE);
    } catch (IOException e) {
      logger.error("Fail to get can execute of {}. ", hdfsPath.toUri(), e);
      return false;
    }
  }

  @Override
  public long getTotalSpace() {
    try {
      return fs.getStatus().getCapacity();
    } catch (IOException e) {
      logger.error("Fail to get total space of {}. ", hdfsPath.toUri(), e);
      return 0L;
    }
  }

  @Override
  public long getUsableSpace() {
    try {
      return fs.getStatus().getUsed();
    } catch (IOException e) {
      logger.error("Fail to get usable space of {}. ", hdfsPath.toUri(), e);
      return 0L;
    }
  }

  @Override
  public java.nio.file.Path toPath() {
    return java.nio.file.Paths.get(hdfsPath.toUri());
  }
}
