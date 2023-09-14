package org.apache.iotdb.hadoop.fileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PositionedReadable;

import java.io.IOException;
import java.io.Serializable;

/** Desc: buffer for {@link org.apache.hadoop.fs.PositionedReadable} */
public class HDFSPositionBuffer implements Serializable {
  private long startPos;
  private final long endPos;
  private int bufferSize;
  private byte[] buffer;

  public HDFSPositionBuffer(Configuration conf, long limit) {
    this.bufferSize =
        conf.getInt(
            "io.file.position.buffer.size",
            262144); // default: 256KB, different with `io.file.buffer.size`
    if (this.bufferSize > limit) {
      this.bufferSize = Long.valueOf(limit).intValue();
    }
    this.startPos = -1L;
    this.endPos = limit - 1;
  }

  public byte[] readFully(PositionedReadable in, long position, int size) throws IOException {
    // if file is too small, cache it
    if (this.endPos < this.bufferSize) {
      if (startPos == -1L) {
        startPos = 0L;
        this.buffer = new byte[bufferSize];
        in.readFully(startPos, buffer);
      }
      byte[] bytes = new byte[size];
      System.arraycopy(buffer, Long.valueOf((position - startPos)).intValue(), bytes, 0, size);
      return bytes;
    }

    // read directly in following scenarios:
    // 1. 128KB read with metadata at the footer
    // 2. file is not big enough to red
    // 3. read size is larger than buffer size
    if (position + 131072 > endPos + 1 || position + size >= endPos + 1 || size > bufferSize) {
      byte[] bytes = new byte[size];
      in.readFully(position, bytes);
      return bytes;
    }

    // init buffer
    if (startPos == -1L) {
      startPos = Math.min(position, endPos + 1 - bufferSize);
      this.buffer = new byte[bufferSize];
      in.readFully(startPos, buffer);
    }

    if (startPos <= position && position + size <= startPos + bufferSize) {
      byte[] bytes = new byte[size];
      System.arraycopy(buffer, Long.valueOf((position - startPos)).intValue(), bytes, 0, size);
      return bytes;
    } else {
      // reinit buffer
      startPos = Math.min(position, endPos + 1 - bufferSize);
      in.readFully(startPos, this.buffer);
      byte[] bytes = new byte[size];
      System.arraycopy(buffer, Long.valueOf((position - startPos)).intValue(), bytes, 0, size);
      return bytes;
    }
  }

  @Override
  public String toString() {
    return "HDFSPositionBuffer{"
        + "startPos="
        + startPos
        + ", endPos="
        + endPos
        + ", bufferSize="
        + bufferSize
        + '}';
  }
}
