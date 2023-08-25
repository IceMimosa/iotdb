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

package org.apache.iotdb.db.pipe.resource;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.utils.FSUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class PipeHardlinkFileDirStartupCleaner {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeHardlinkFileDirStartupCleaner.class);

  /**
   * Delete the data directory and all of its subdirectories that contain the
   * PipeConfig.PIPE_TSFILE_DIR_NAME directory.
   */
  public static void clean() {
    for (final String dataDir : IoTDBDescriptor.getInstance().getConfig().getDataDirs()) {

      for (final File file :
          listFilesAndDirs(dataDir, DirectoryFileFilter.INSTANCE, DirectoryFileFilter.INSTANCE)) {
        if (file.isDirectory()
            && file.getName().equals(PipeConfig.getInstance().getPipeHardlinkBaseDirName())) {
          LOGGER.info("pipe hardlink dir found, deleting it: {}, result: {}", file, file.delete());
        }
      }
    }
  }

  public static Collection<File> listFilesAndDirs(
      String dataDir, IOFileFilter fileFilter, IOFileFilter dirFilter) {
    File dataDirFile = FSFactoryProducer.getFSFactory().getFile(dataDir);
    LOGGER.info("dataDirFile is {}", dataDirFile);
    if (FSUtils.isLocal(dataDirFile)) {
      return FileUtils.listFilesAndDirs(dataDirFile, fileFilter, dirFilter);
    }
    return Collections.emptyList();
    // TODO: too slow on start
    // return listHdfsFilePath(dataDirFile, fileFilter, dirFilter);
  }

  private static List<File> listHdfsFilePath(
      File hdfsFilePath, IOFileFilter fileFilter, IOFileFilter dirFilter) {
    List<File> filePathList = new ArrayList<>();
    Queue<File> fileQueue = new LinkedList<>();
    fileQueue.add(hdfsFilePath);
    while (!fileQueue.isEmpty()) {
      File file = fileQueue.remove();

      if (file.isFile()) {
        if (fileFilter.accept(file)) {
          filePathList.add(file);
        }
      } else {
        if (dirFilter.accept(file)) {
          File[] fileStatus = file.listFiles((FileFilter) fileFilter);
          if (fileStatus != null) {
            Collections.addAll(fileQueue, fileStatus);
          }
        }
      }
    }
    return filePathList;
  }

  private PipeHardlinkFileDirStartupCleaner() {
    // util class
  }
}
