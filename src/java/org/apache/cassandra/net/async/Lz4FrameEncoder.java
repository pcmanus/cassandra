/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.net.async;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.compression.CompressionException;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;

import static org.apache.cassandra.net.async.Lz4Constants.BLOCK_TYPE_COMPRESSED;
import static org.apache.cassandra.net.async.Lz4Constants.BLOCK_TYPE_NON_COMPRESSED;
import static org.apache.cassandra.net.async.Lz4Constants.CHECKSUM_OFFSET;
import static org.apache.cassandra.net.async.Lz4Constants.COMPRESSED_LENGTH_OFFSET;
import static org.apache.cassandra.net.async.Lz4Constants.COMPRESSION_LEVEL_BASE;
import static org.apache.cassandra.net.async.Lz4Constants.DECOMPRESSED_LENGTH_OFFSET;
import static org.apache.cassandra.net.async.Lz4Constants.DEFAULT_BLOCK_SIZE;
import static org.apache.cassandra.net.async.Lz4Constants.HEADER_LENGTH;
import static org.apache.cassandra.net.async.Lz4Constants.MAGIC_NUMBER;
import static org.apache.cassandra.net.async.Lz4Constants.MAX_BLOCK_SIZE;
import static org.apache.cassandra.net.async.Lz4Constants.MIN_BLOCK_SIZE;
import static org.apache.cassandra.net.async.Lz4Constants.TOKEN_OFFSET;

/**
 * Compresses a {@link ByteBuf} using the LZ4 format.
 *
 * See original <a href="http://code.google.com/p/lz4/">LZ4 website</a>
 * and <a href="http://fastcompression.blogspot.ru/2011/05/lz4-explained.html">LZ4 block format</a>
 * for full description.
 *
 * Since the original LZ4 block format does not contains size of compressed block and size of original data
 * this encoder uses format like <a href="https://github.com/idelpivnitskiy/lz4-java">LZ4 Java</a> library
 * written by Adrien Grand and approved by Yann Collet (author of original LZ4 library).
 *
 *  * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *     * * * * * * * * * *
 *  * Magic * Token *  Compressed *  Decompressed *  Checksum *  +  *  LZ4 compressed *
 *  *       *       *    length   *     length    *           *     *      block      *
 *  * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *     * * * * * * * * * *
 *
 *  Note: backported from netty 4.1, but customized/optimized for cassandra by using
 *  {@link CRC32} as the {@link Checksum} so we can call the {@link ByteBuffer} related function(s).
 */
class Lz4FrameEncoder extends MessageToByteEncoder<ByteBuf>
{
    /**
     * Underlying compressor in use.
     */
    private final LZ4Compressor compressor;

    /**
     * Underlying checksum calculator in use.
     */
    private final CRC32 checksum;

    /**
     * Compression level of current LZ4 encoder.
     */
    private final int compressionLevel;

    /**
     * Indicates if the compressed stream has been finished.
     */
    private boolean finished;

    /**
     * Creates the fastest LZ4 encoder with default block size (64 KB)
     * and xxhash hashing for Java, based on Yann Collet's work available at
     * <a href="http://code.google.com/p/xxhash/">Google Code</a>.
     */
    public Lz4FrameEncoder()
    {
        this(false);
    }

    /**
     * Creates a new LZ4 encoder with hight or fast compression, default block size (64 KB)
     * and xxhash hashing for Java, based on Yann Collet's work available at
     * <a href="http://code.google.com/p/xxhash/">Google Code</a>.
     *
     * @param highCompressor  if {@code true} codec will use compressor which requires more memory
     *                        and is slower but compresses more efficiently
     */
    public Lz4FrameEncoder(boolean highCompressor)
    {
        this(LZ4Factory.fastestInstance(), highCompressor, DEFAULT_BLOCK_SIZE, new CRC32());
    }

    /**
     * Creates a new customizable LZ4 encoder.
     *
     * @param factory         user customizable {@link net.jpountz.lz4.LZ4Factory} instance
     *                        which may be JNI bindings to the original C implementation, a pure Java implementation
     *                        or a Java implementation that uses the {@link sun.misc.Unsafe}
     * @param highCompressor  if {@code true} codec will use compressor which requires more memory
     *                        and is slower but compresses more efficiently
     * @param blockSize       the maximum number of bytes to try to compress at once,
     *                        must be >= 64 and <= 32 M
     * @param checksum        the {@link Checksum} instance to use to check data for integrity
     */
    public Lz4FrameEncoder(LZ4Factory factory, boolean highCompressor, int blockSize, CRC32 checksum)
    {
        if (factory == null)
            throw new NullPointerException("factory");
        if (checksum == null)
            throw new NullPointerException("checksum");

        compressor = highCompressor ? factory.highCompressor() : factory.fastCompressor();
        this.checksum = checksum;

        compressionLevel = compressionLevel(blockSize);
    }

    /**
     * Calculates compression level on the basis of block size.
     */
    private static int compressionLevel(int blockSize)
    {
        if (blockSize < MIN_BLOCK_SIZE || blockSize > MAX_BLOCK_SIZE)
        {
            throw new IllegalArgumentException(String.format(
                    "blockSize: %d (expected: %d-%d)", blockSize, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE));
        }
        int compressionLevel = 32 - Integer.numberOfLeadingZeros(blockSize - 1); // ceil of log2
        compressionLevel = Math.max(0, compressionLevel - COMPRESSION_LEVEL_BASE);
        return compressionLevel;
    }

    @Override
    protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, ByteBuf msg, boolean preferDirect) throws Exception
    {
        int maxCompressedLength = HEADER_LENGTH + compressor.maxCompressedLength(msg.readableBytes());
        if (preferDirect)
            return ctx.alloc().ioBuffer(maxCompressedLength, maxCompressedLength);
        else
            return ctx.alloc().heapBuffer(maxCompressedLength, maxCompressedLength);
    }

    @Override
    public void encode(ChannelHandlerContext ctx, ByteBuf in, ByteBuf out) throws Exception
    {
        if (finished)
            return;

        compress(in, out);
    }

    private void compress(ByteBuf in, ByteBuf out)
    {
        final int uncompressedLength = in.readableBytes();
        checksum.reset();
        checksum.update(in.nioBuffer(in.readerIndex(), uncompressedLength));
        final int check = (int) checksum.getValue();

        ByteBuffer rawSlice = in.nioBuffer(in.readerIndex(), uncompressedLength);
        ByteBuffer compressedSlice = out.nioBuffer(0, out.capacity());
        compressedSlice.position(HEADER_LENGTH);
        try
        {
            compressor.compress(rawSlice, compressedSlice);
        }
        catch (LZ4Exception e)
        {
            throw new CompressionException(e);
        }

        int compressedLength = compressedSlice.position() - HEADER_LENGTH;
        final int blockType;

        // on the off-chance that the compressed size is actually larger than the raw size, just use the raw data.
        // we consider the cost of the memcpy here less than the cost of the decompress on the peer's side
        // (plus, we save a few bytes on the network).
        if (compressedLength < uncompressedLength)
        {
            blockType = BLOCK_TYPE_COMPRESSED;
            in.skipBytes(rawSlice.position());
        }
        else
        {
            blockType = BLOCK_TYPE_NON_COMPRESSED;
            compressedLength = uncompressedLength;
            out.writerIndex(HEADER_LENGTH);
            in.readBytes(out, HEADER_LENGTH, uncompressedLength);
        }

        out.setLong(0, MAGIC_NUMBER);
        out.setByte(TOKEN_OFFSET, (byte) (blockType | compressionLevel));
        writeIntLE(compressedLength, out, COMPRESSED_LENGTH_OFFSET);
        writeIntLE(uncompressedLength, out, DECOMPRESSED_LENGTH_OFFSET);
        writeIntLE(check, out, CHECKSUM_OFFSET);
        out.writerIndex(HEADER_LENGTH + compressedLength);
    }

    /**
     * Writes {@code int} value into the byte buffer with little-endian format.
     */
    private static void writeIntLE(int i, ByteBuf buf, int off)
    {
        buf.setByte(off++, (byte) i);
        buf.setByte(off++, (byte) (i >>> 8));
        buf.setByte(off++, (byte) (i >>> 16));
        buf.setByte(off,   (byte) (i >>> 24));
    }

    private void close(ChannelHandlerContext ctx)
    {
        if (finished)
            return;

        final ByteBuf footer = ctx.alloc().ioBuffer(HEADER_LENGTH);

        footer.setLong(0, MAGIC_NUMBER);
        footer.setByte(TOKEN_OFFSET, (byte) (BLOCK_TYPE_NON_COMPRESSED | compressionLevel));
        writeIntLE(0, footer, COMPRESSED_LENGTH_OFFSET);
        writeIntLE(0, footer, DECOMPRESSED_LENGTH_OFFSET);
        writeIntLE(0, footer, CHECKSUM_OFFSET);
        footer.writerIndex(HEADER_LENGTH);

        ctx.writeAndFlush(footer);
        finished = true;
    }

    public void close(ChannelHandlerContext ctx, ChannelPromise promise)
    {
        close(ctx);
        ctx.close(promise);
    }
}
