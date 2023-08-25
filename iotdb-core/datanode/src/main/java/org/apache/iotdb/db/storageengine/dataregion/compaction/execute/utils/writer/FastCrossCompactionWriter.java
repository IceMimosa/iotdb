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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.utils.FSUtils;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class FastCrossCompactionWriter extends AbstractCrossCompactionWriter {
  // Only used for fast compaction performer
  protected Map<TsFileResource, TsFileSequenceReader> readerMap;

  private final boolean local;
  private final boolean[] hasInitStartChunkGroup;

  public FastCrossCompactionWriter(
      List<TsFileResource> targetResources,
      List<TsFileResource> seqSourceResources,
      Map<TsFileResource, TsFileSequenceReader> readerMap)
      throws IOException {
    super(targetResources, seqSourceResources);
    local = FSUtils.getFSType(targetResources.get(0).getTsFile()) == FSType.LOCAL;
    hasInitStartChunkGroup = new boolean[targetFileWriters.size()];
    this.readerMap = readerMap;
  }

  @Override
  public void write(TimeColumn timestamps, Column[] columns, int subTaskId, int batchSize)
      throws IOException {
    throw new RuntimeException("Does not support this method in FastCrossCompactionWriter");
  }

  @Override
  protected TsFileSequenceReader getFileReader(TsFileResource resource) {
    return readerMap.get(resource);
  }

  @Override
  public void startChunkGroup(String deviceId, boolean isAlign) throws IOException {
    if (local) {
      super.startChunkGroup(deviceId, isAlign);
    } else {
      this.deviceId = deviceId;
      this.isAlign = isAlign;
      this.seqFileIndexArray = new int[subTaskNum];
      checkIsDeviceExistAndGetDeviceEndTime();
      for (int i = 0; i < targetFileWriters.size(); i++) {
        // chunkGroupHeaderSize = targetFileWriters.get(i).startChunkGroup(deviceId);
        targetFileWriters.get(i).startInitChunkGroup(deviceId);
      }
    }
  }

  @Override
  public void endChunkGroup() throws IOException {
    if (local) {
      super.endChunkGroup();
    } else {
      for (int i = 0; i < seqTsFileResources.size(); i++) {
        CompactionTsFileWriter targetFileWriter = targetFileWriters.get(i);
        if (isDeviceExistedInTargetFiles[i]) {
          // update resource
          CompactionUtils.updateResource(targetResources.get(i), targetFileWriter, deviceId);
          targetFileWriter.endChunkGroup();
        } else {
          if (hasInitStartChunkGroup[i]) {
            targetFileWriter.truncate(targetFileWriter.getPos() - chunkGroupHeaderSize);
          }
        }
        hasInitStartChunkGroup[i] = false;
        isDeviceExistedInTargetFiles[i] = false;
      }
      seqFileIndexArray = null;
    }
  }

  /**
   * Flush nonAligned chunk to tsfile directly. Return whether the chunk is flushed to tsfile
   * successfully or not. Return false if the unsealed chunk is too small or the end time of chunk
   * exceeds the end time of file, else return true.
   */
  @Override
  public boolean flushNonAlignedChunk(Chunk chunk, ChunkMetadata chunkMetadata, int subTaskId)
      throws IOException {
    int fileIndex = seqFileIndexArray[subTaskId];
    boolean checkIsChunkSatisfied = checkIsChunkSatisfied(chunkMetadata, fileIndex, subTaskId);
    if (checkIsChunkSatisfied) {
      if (!local && !hasInitStartChunkGroup[fileIndex]) {
        chunkGroupHeaderSize = targetFileWriters.get(fileIndex).startChunkGroup(deviceId);
        hasInitStartChunkGroup[fileIndex] = true;
      }
    }
    checkTimeAndMayFlushChunkToCurrentFile(chunkMetadata.getStartTime(), subTaskId);
    if (!checkIsChunkSatisfied) {
      // if unsealed chunk is not large enough or chunk.endTime > file.endTime, then deserialize the
      // chunk
      return false;
    }

    flushNonAlignedChunkToFileWriter(
        targetFileWriters.get(fileIndex), chunk, chunkMetadata, subTaskId);

    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    lastTime[subTaskId] = chunkMetadata.getEndTime();
    return true;
  }

  /**
   * Flush aligned chunk to tsfile directly. Return whether the chunk is flushed to tsfile
   * successfully or not. Return false if the unsealed chunk is too small or the end time of chunk
   * exceeds the end time of file, else return true. Notice: if sub-value measurement is null, then
   * flush empty value chunk.
   */
  @Override
  public boolean flushAlignedChunk(
      Chunk timeChunk,
      IChunkMetadata timeChunkMetadata,
      List<Chunk> valueChunks,
      List<IChunkMetadata> valueChunkMetadatas,
      int subTaskId)
      throws IOException {
    int fileIndex = seqFileIndexArray[subTaskId];
    boolean checkIsChunkSatisfied = checkIsChunkSatisfied(timeChunkMetadata, fileIndex, subTaskId);
    if (checkIsChunkSatisfied) {
      if (!local && !hasInitStartChunkGroup[fileIndex]) {
        chunkGroupHeaderSize = targetFileWriters.get(fileIndex).startChunkGroup(deviceId);
        hasInitStartChunkGroup[fileIndex] = true;
      }
    }
    checkTimeAndMayFlushChunkToCurrentFile(timeChunkMetadata.getStartTime(), subTaskId);
    if (!checkIsChunkSatisfied) {
      // if unsealed chunk is not large enough or chunk.endTime > file.endTime, then deserialize the
      // chunk
      return false;
    }

    flushAlignedChunkToFileWriter(
        targetFileWriters.get(fileIndex),
        timeChunk,
        timeChunkMetadata,
        valueChunks,
        valueChunkMetadatas,
        subTaskId);

    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    lastTime[subTaskId] = timeChunkMetadata.getEndTime();
    return true;
  }

  /**
   * Flush aligned page to tsfile directly. Return whether the page is flushed to tsfile
   * successfully or not. Return false if the unsealed page is too small or the end time of page
   * exceeds the end time of file, else return true. Notice: if sub-value measurement is null, then
   * flush empty value page.
   *
   * @throws IOException if io errors occurred
   * @throws PageException if errors occurred when write data page header
   */
  public boolean flushAlignedPage(
      ByteBuffer compressedTimePageData,
      PageHeader timePageHeader,
      List<ByteBuffer> compressedValuePageDatas,
      List<PageHeader> valuePageHeaders,
      int subTaskId)
      throws IOException, PageException {
    int fileIndex = seqFileIndexArray[subTaskId];
    boolean checkIsPageSatisfied = checkIsPageSatisfied(timePageHeader, fileIndex, subTaskId);
    if (checkIsPageSatisfied) {
      if (!local && !hasInitStartChunkGroup[fileIndex]) {
        chunkGroupHeaderSize = targetFileWriters.get(fileIndex).startChunkGroup(deviceId);
        hasInitStartChunkGroup[fileIndex] = true;
      }
    }
    checkTimeAndMayFlushChunkToCurrentFile(timePageHeader.getStartTime(), subTaskId);
    if (!checkIsPageSatisfied) {
      // unsealed page is too small or page.endTime > file.endTime, then deserialize the page
      return false;
    }

    flushAlignedPageToChunkWriter(
        (AlignedChunkWriterImpl) chunkWriters[subTaskId],
        compressedTimePageData,
        timePageHeader,
        compressedValuePageDatas,
        valuePageHeaders,
        subTaskId);

    // check chunk size and may open a new chunk
    checkChunkSizeAndMayOpenANewChunk(
        targetFileWriters.get(fileIndex), chunkWriters[subTaskId], subTaskId);

    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    lastTime[subTaskId] = timePageHeader.getEndTime();
    return true;
  }

  /**
   * Flush nonAligned page to tsfile directly. Return whether the page is flushed to tsfile
   * successfully or not. Return false if the unsealed page is too small or the end time of page
   * exceeds the end time of file, else return true.
   *
   * @throws IOException if io errors occurred
   * @throws PageException if errors occurred when write data page header
   */
  public boolean flushNonAlignedPage(
      ByteBuffer compressedPageData, PageHeader pageHeader, int subTaskId)
      throws IOException, PageException {
    int fileIndex = seqFileIndexArray[subTaskId];
    boolean checkIsPageSatisfied = checkIsPageSatisfied(pageHeader, fileIndex, subTaskId);
    if (checkIsPageSatisfied) {
      if (!local && !hasInitStartChunkGroup[fileIndex]) {
        chunkGroupHeaderSize = targetFileWriters.get(fileIndex).startChunkGroup(deviceId);
        hasInitStartChunkGroup[fileIndex] = true;
      }
    }
    checkTimeAndMayFlushChunkToCurrentFile(pageHeader.getStartTime(), subTaskId);
    if (!checkIsPageSatisfied) {
      // unsealed page is too small or page.endTime > file.endTime, then deserialize the page
      return false;
    }

    flushNonAlignedPageToChunkWriter(
        (ChunkWriterImpl) chunkWriters[subTaskId], compressedPageData, pageHeader, subTaskId);

    // check chunk size and may open a new chunk
    checkChunkSizeAndMayOpenANewChunk(
        targetFileWriters.get(fileIndex), chunkWriters[subTaskId], subTaskId);

    isDeviceExistedInTargetFiles[fileIndex] = true;
    isEmptyFile[fileIndex] = false;
    lastTime[subTaskId] = pageHeader.getEndTime();
    return true;
  }

  @Override
  public void write(TimeValuePair timeValuePair, int subTaskId) throws IOException {
    int fileIndex = seqFileIndexArray[subTaskId];
    if (!local && !hasInitStartChunkGroup[fileIndex]) {
      chunkGroupHeaderSize = targetFileWriters.get(fileIndex).startChunkGroup(deviceId);
      hasInitStartChunkGroup[fileIndex] = true;
    }
    super.write(timeValuePair, subTaskId);
  }

  private boolean checkIsChunkSatisfied(
      IChunkMetadata chunkMetadata, int fileIndex, int subTaskId) {
    boolean isUnsealedChunkLargeEnough =
        chunkWriters[subTaskId].checkIsChunkSizeOverThreshold(
            chunkSizeLowerBoundInCompaction, chunkPointNumLowerBoundInCompaction, true);
    // if unsealed chunk is not large enough or chunk.endTime > file.endTime, then return false
    return isUnsealedChunkLargeEnough
        && (chunkMetadata.getEndTime() <= currentDeviceEndTime[fileIndex]
            || fileIndex == targetFileWriters.size() - 1);
  }

  private boolean checkIsPageSatisfied(PageHeader pageHeader, int fileIndex, int subTaskId) {
    boolean isUnsealedPageLargeEnough =
        chunkWriters[subTaskId].checkIsUnsealedPageOverThreshold(
            pageSizeLowerBoundInCompaction, pagePointNumLowerBoundInCompaction, true);
    // unsealed page is too small or page.endTime > file.endTime, then return false
    return isUnsealedPageLargeEnough
        && (pageHeader.getEndTime() <= currentDeviceEndTime[fileIndex]
            || fileIndex == targetFileWriters.size() - 1);
  }
}
