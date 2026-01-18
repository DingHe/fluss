/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.record;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.record.FileLogInputStream.FileChannelLogRecordBatch;
import org.apache.fluss.utils.AbstractIterator;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * An entity used to describe multiple {@link LogRecordBatch}s in File.
 *
 * @since 0.1
 */
// 直接负责与文件系统交互。它基于 Apache Kafka 的 FileRecords 优化而来，提供了高性能的消息持久化、检索和切片功能。
// FileLogRecords 是 .log 数据文件的封装器。它的主要作用包括：
// 物理存储管理：管理底层的 FileChannel，负责消息在磁盘上的实际读写。
// 追加写入（Append）：将内存中的消息批次（MemoryLogRecords）顺序写入磁盘。
// 逻辑视图（Slice）：支持在不拷贝数据的情况下，通过指定起始位置和大小来创建一个日志文件的“视图”。
// 消息检索：提供基于 Offset（偏移量）和 Timestamp（时间戳）的物理位置查找功能。

@PublicEvolving
public class FileLogRecords implements LogRecords, Closeable {
    // 标识当前对象是一个完整的文件还是文件的一个切片（视图）。
    private final boolean isSlice;
    // 当前记录集的起始物理字节位置（主要用于切片）
    private final int start;
    // 当前记录集的结束物理字节位置。
    private final int end;

    // mutable state
    // 当前记录集包含的总字节数。
    private final AtomicInteger size;
    // 对应的磁盘文件引用。
    private volatile File file;
    // 用于执行底层 I/O 操作（读、写、截断、强制刷新）的通道。
    private final FileChannel channel;

    FileLogRecords(File file, FileChannel channel, int start, int end, boolean isSlice)
            throws IOException {
        this.file = file;
        this.channel = channel;
        this.start = start;
        this.end = end;
        this.isSlice = isSlice;

        size = new AtomicInteger();

        if (isSlice) {
            // don't check the file size if this is just a slice view
            size.set(end - start);
        } else {
            if (channel.size() > Integer.MAX_VALUE) {
                throw new FlussRuntimeException(
                        "The size of segment "
                                + file
                                + " ("
                                + channel.size()
                                + ") is larger than the maximum allowed segment size of "
                                + Integer.MAX_VALUE);
            }

            int limit = Math.min((int) channel.size(), end);
            size.set(limit - start);

            // if this is not a slice, update the file pointer to the end of the file
            // set the file position to the last byte in the file
            channel.position(limit);
        }
    }

    /**
     * Get the underlying file.
     *
     * @return The file
     */
    public File file() {
        return file;
    }

    /**
     * Get the underlying file channel.
     *
     * @return The file channel
     */
    public FileChannel channel() {
        return channel;
    }

    /**
     * Read log batches into the given buffer until there are no bytes remaining in the buffer or
     * the end of the file is reached.
     *
     * @param buffer The buffer to write the batches to
     * @param position Position in the buffer to read from
     * @throws IOException If an I/O error occurs, see {@link FileChannel#read(ByteBuffer, long)}
     *     for details on the possible exceptions
     */
    // 负责将磁盘数据加载到内存缓冲区（ByteBuffer）的核心方法。它通常在执行数据拉取（Fetch）或数据校验时被调用。
    // ByteBuffer buffer: 目标缓冲区。数据将从磁盘读取并填充到这个 buffer 中。
    // int position: 逻辑起始位置。注意，这个 position 是相对于当前 FileLogRecords 对象的起始位置而言的，而不是绝对的文件物理位置。
    public void readInto(ByteBuffer buffer, int position) throws IOException {
        FileUtils.readFully(channel, buffer, position + this.start);
        buffer.flip();
    }

    /**
     * Return a slice of records from this instance, which is a view into this set starting from the
     * given position and with the given size limit.
     *
     * <p>If the size is beyond the end of the file, the end will be based on the size of the file
     * at the time of the read.
     *
     * <p>If this message set is already sliced, the position will be taken relative to that
     * slicing.
     *
     * @param position The start position to begin the read from
     * @param size The number of bytes after the start position to include
     * @return A sliced wrapper on this message set limited based on the given position and size
     */
    // 核心目的是范围限定。 当上层组件（如 LogSegment）需要读取日志文件的一部分（例如：从偏移量 A 到偏移量 B 之间的所有消息）时，它会调用此方法。
    // 返回的新对象虽然共享同一个底层文件通道，但其操作范围被严格限制在指定的 position 和 size 之内。
    public FileLogRecords slice(int position, int size) throws IOException {
        // 检查 position 是否为负数或超过了当前文件/切片的限制。
        int availableBytes = availableBytes(position, size);
        int startPosition = this.start + position;
        return new FileLogRecords(
                file, channel, startPosition, startPosition + availableBytes, true);
    }
    // 本质是一个边界安全检查器，确保任何读操作或切片操作都不会超出文件的物理边界或当前对象的逻辑边界。
    private int availableBytes(int position, int size) {
        // Cache current size in case concurrent write changes it
        // 获取当前记录集的总字节数。
        int currentSizeInBytes = sizeInBytes();
        // 起始偏移量不能为负数。
        if (position < 0) {
            throw new IllegalArgumentException(
                    "Invalid position: " + position + " in read from " + this);
        }
        // 校验起始位置是否超出逻辑结尾
        if (position > currentSizeInBytes - start) {
            throw new IllegalArgumentException(
                    "Slice from position " + position + " exceeds end position of " + this);
        }

        if (size < 0) {
            throw new IllegalArgumentException("Invalid size: " + size + " in read from " + this);
        }

        int end = this.start + position + size;
        // Handle integer overflow or if end is beyond the end of the file
        if (end < 0 || end > start + currentSizeInBytes) {
            end = this.start + currentSizeInBytes;
        }
        return end - (this.start + position);
    }
    // 实现了“顺序写”逻辑。它接收一组已经格式化好的内存记录，利用操作系统的 FileChannel 将其一次性追加到文件的末尾，并更新当前日志段的大小。
    public int append(MemoryLogRecords records) throws IOException {
        if (records.sizeInBytes() > Integer.MAX_VALUE - size.get()) {
            throw new IllegalArgumentException(
                    "Append of size "
                            + records.sizeInBytes()
                            + " bytes is too large for segment with current file position at "
                            + size.get());
        }
        // 物理执行写入
        int written = records.writeFullyTo(channel);
        size.getAndAdd(written);
        return written;
    }

    /** Commit all written data to the physical disk. */
    public void flush() throws IOException {
        channel.force(true);
    }

    /** Close this record set. */
    public void close() throws IOException {
        flush();
        trim();
        channel.close();
    }

    /**
     * Close file handlers used by the FileChannel but don't write to disk. This is used when the
     * disk may have failed.
     */
    public void closeHandlers() throws IOException {
        channel.close();
    }

    /**
     * Delete this message set from the filesystem.
     *
     * @throws IOException if deletion fails due to an I/O error
     * @return {@code true} if the file was deleted by this method; {@code false} if the file could
     *     not be deleted because it did not exist
     */
    // 负责彻底从操作系统的文件系统中移除当前的 .log 文件。它采用“先关闭、后删除”的策略，确保在删除过程中不会因为文件句柄（File Handle）被占用而导致删除失败
    public boolean deleteIfExists() throws IOException {
        IOUtils.closeQuietly(channel, "FileChannel");
        return Files.deleteIfExists(file.toPath());
    }

    /** Trim file when close or roll to next file. */
    // 核心作用是将底层磁盘文件的物理大小调整为与当前数据的逻辑大小完全一致。
    public void trim() throws IOException {
        truncateTo(sizeInBytes());
    }

    /**
     * Update the parent directory (to be used with caution since this does not reopen the file
     * channel).
     *
     * @param parentDir The new parent directory
     */
    public void updateParentDir(File parentDir) {
        this.file = new File(parentDir, file.getName());
    }

    /**
     * Truncate this file message set to the given size in bytes. Note that this API does no
     * checking that the given size falls on a valid message boundary. In some versions of the JDK
     * truncating to the same size as the file message set will cause an update of the files mtime,
     * so truncate is only performed if the targetSize is smaller than the size of the underlying
     * FileChannel. It is expected that no other threads will do writes to the log when this
     * function is called.
     *
     * @param targetSize The size to truncate to. Must be between 0 and sizeInBytes.
     * @return The number of bytes truncated off
     */
    // 对 .log 文件的**截断（Truncate）**操作。
    // 它允许将文件收缩到指定的大小，通常用于清理文件末尾的空白预分配空间，或者在故障恢复时删除损坏的不完整消息批次。
    public int truncateTo(int targetSize) throws IOException {
        int originalSize = sizeInBytes();
        if (targetSize > originalSize || targetSize < 0) {
            throw new FlussRuntimeException(
                    "Attempt to truncate log segment "
                            + file
                            + " to "
                            + targetSize
                            + " bytes failed, "
                            + " size of this log segment is "
                            + originalSize
                            + " bytes.");
        }
        if (targetSize < (int) channel.size()) {
            channel.truncate(targetSize);
            size.set(targetSize);
        }
        return originalSize - targetSize;
    }

    public int sizeInBytes() {
        return size.get();
    }
    // 是 FileLogRecords 提供给上层业务（如查询、消费、数据校验）的核心数据读取入口。
    @Override
    public Iterable<LogRecordBatch> batches() {
        Iterable<FileChannelLogRecordBatch> it = batchesFrom(start);
        return () -> {
            Iterator<FileChannelLogRecordBatch> iterator = it.iterator();
            return new Iterator<LogRecordBatch>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public LogRecordBatch next() {
                    return iterator.next();
                }
            };
        };
    }

    /**
     * Get an iterator over the record batches in the file, starting at a specific position. This is
     * similar to {@link #batches()} except that callers specify a particular position to start
     * reading the batches from. This method must be used with caution: the start position passed in
     * must be a known start of a batch.
     *
     * @return An iterator over batches starting from {@code start}
     */
    private Iterable<FileChannelLogRecordBatch> batchesFrom(final int start) {
        return () -> {
            try {
                return batchIterator(start);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public FileChannelChunk toChunk() {
        return new FileChannelChunk(channel, start, sizeInBytes());
    }

    private AbstractIterator<FileChannelLogRecordBatch> batchIterator(int start)
            throws IOException {
        final int end;
        if (isSlice) {
            end = this.end;
        } else {
            end = this.sizeInBytes();
        }

        FileLogInputStream inputStream = new FileLogInputStream(this, start, end);
        return new LogRecordBatchIterator<>(inputStream);
    }

    /**
     * Rename the file that backs this message set.
     *
     * @throws IOException if rename fails.
     */
    public void renameTo(File f) throws IOException {
        try {
            FileUtils.atomicMoveWithFallback(file.toPath(), f.toPath(), false);
        } finally {
            this.file = f;
        }
    }

    /**
     * Search forward for the file position of the last offset that is greater than or equal to the
     * target offset and return its physical position and the size of the message (including log
     * overhead) at the returned offset. If no such offsets are found, return null.
     *
     * @param targetOffset The offset to search for.
     * @param startingPosition The starting position in the file to begin searching from.
     */
    public LogOffsetPosition searchForOffsetWithSize(long targetOffset, int startingPosition) {
        for (FileChannelLogRecordBatch batch : batchesFrom(startingPosition)) {
            long offset = batch.lastLogOffset();
            if (offset >= targetOffset) {
                return new LogOffsetPosition(offset, batch.position(), batch.sizeInBytes());
            }
        }
        return null;
    }

    /**
     * Search forward for the first batch that meets the following requirements:
     *
     * <pre>
     *  - recordBatch's commit timestamp is greater than or equals to the targetTimestamp.
     *  - recordBatch's position in the log file is greater than or equals to the startingPosition.
     *  - recordBatch's offset is greater than or equals to the startingOffset.
     * </pre>
     *
     * @param targetTimestamp The timestamp to search for.
     * @param startingPosition The starting position to search.
     * @param startingOffset The starting offset to search.
     * @return The timestamp and offset of the message found. Null if no message is found.
     */
    public @Nullable TimestampAndOffset searchForTimestamp(
            long targetTimestamp, int startingPosition, long startingOffset) {
        for (LogRecordBatch batch : batchesFrom(startingPosition)) {
            long commitTimestamp = batch.commitTimestamp();
            long baseLogOffset = batch.baseLogOffset();
            if (commitTimestamp >= targetTimestamp && baseLogOffset >= startingOffset) {
                return new TimestampAndOffset(commitTimestamp, baseLogOffset);
            }
        }
        return null;
    }

    /** Return the largest timestamp after a given position in this file. */
    public TimestampAndOffset largestTimestampAfter(int startPosition) {
        long maxTimestamp = -1L;
        long startOffsetOfMaxTimestamp = -1L;

        for (LogRecordBatch batch : batchesFrom(startPosition)) {
            long commitTimestamp = batch.commitTimestamp();
            if (commitTimestamp > maxTimestamp) {
                maxTimestamp = commitTimestamp;
                startOffsetOfMaxTimestamp = batch.baseLogOffset();
            }
        }

        return new TimestampAndOffset(maxTimestamp, startOffsetOfMaxTimestamp);
    }

    @Override
    public String toString() {
        return "FileRecords(size="
                + sizeInBytes()
                + ", file="
                + file
                + ", start="
                + start
                + ", end="
                + end
                + ")";
    }

    public static FileLogRecords open(
            File file,
            boolean mutable,
            boolean fileAlreadyExists,
            int initFileSize,
            boolean preallocate)
            throws IOException {
        FileChannel channel =
                openChannel(file, mutable, fileAlreadyExists, initFileSize, preallocate);
        int end = (!fileAlreadyExists && preallocate) ? 0 : Integer.MAX_VALUE;
        return new FileLogRecords(file, channel, 0, end, false);
    }

    public static FileLogRecords open(
            File file, boolean fileAlreadyExists, int initFileSize, boolean preallocate)
            throws IOException {
        return open(file, true, fileAlreadyExists, initFileSize, preallocate);
    }

    public static FileLogRecords open(File file, boolean mutable) throws IOException {
        return open(file, mutable, false, 0, false);
    }

    public static FileLogRecords open(File file) throws IOException {
        return open(file, true);
    }

    /**
     * Open a channel for the given file For windows NTFS and some old LINUX file system, set
     * preallocate to true and initFileSize with one value (for example 512 * 1025 *1024 ) can
     * improve the fluss produce performance.
     *
     * @param file File path
     * @param mutable mutable
     * @param fileAlreadyExists File already exists or not
     * @param initFileSize The size used for pre allocate file, for example 512 * 1025 *1024
     * @param preallocate Pre-allocate file or not, gotten from configuration.
     */
    private static FileChannel openChannel(
            File file,
            boolean mutable,
            boolean fileAlreadyExists,
            int initFileSize,
            boolean preallocate)
            throws IOException {
        if (mutable) {
            if (fileAlreadyExists || !preallocate) {
                return FileChannel.open(
                        file.toPath(),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE);
            } else {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                randomAccessFile.setLength(initFileSize);
                return randomAccessFile.getChannel();
            }
        } else {
            return FileChannel.open(file.toPath());
        }
    }

    /**
     * A position in a log file.
     *
     * @since 0.1
     */
    @PublicEvolving
    public static class LogOffsetPosition {
        public final long offset;
        public final int position;
        public final int size;

        public LogOffsetPosition(long offset, int position, int size) {
            this.offset = offset;
            this.position = position;
            this.size = size;
        }

        public long getOffset() {
            return offset;
        }

        public int getPosition() {
            return position;
        }

        public int getSize() {
            return size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            LogOffsetPosition that = (LogOffsetPosition) o;

            return offset == that.offset && position == that.position && size == that.size;
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(offset);
            result = 31 * result + position;
            result = 31 * result + size;
            return result;
        }

        @Override
        public String toString() {
            return "LogOffsetPosition("
                    + "offset="
                    + offset
                    + ", position="
                    + position
                    + ", size="
                    + size
                    + ')';
        }
    }
}
