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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.compression.DecompressionException;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static org.apache.cassandra.net.async.Lz4Constants.*;

/**
 * Uncompresses a {@link ByteBuf} encoded with the LZ4 format.
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
class Lz4FrameDecoder extends ByteToMessageDecoder
{
    /**
     * Current state of stream.
     */
    private enum State
    {
        INIT_BLOCK,
        DECOMPRESS_DATA,
        FINISHED,
        CORRUPTED
    }

    private State currentState = State.INIT_BLOCK;

    /**
     * Underlying decompressor in use.
     */
    private final LZ4FastDecompressor decompressor;

    /**
     * Underlying checksum calculator in use.
     */
    private final CRC32 checksum;

    /**
     * Type of current block.
     */
    private int blockType;

    /**
     * Compressed length of current incoming block.
     */
    private int compressedLength;

    /**
     * Decompressed length of current incoming block.
     */
    private int decompressedLength;

    /**
     * Checksum value of current incoming block.
     */
    private int currentChecksum;

    /**
     * Creates the fastest LZ4 decoder.
     *
     * Note that by default, validation of the checksum header in each chunk is
     * DISABLED for performance improvements. If performance is less of an issue,
     * or if you would prefer the safety that checksum validation brings, please
     * use the {@link #Lz4FrameDecoder(boolean)} constructor with the argument
     * set to {@code true}.
     */
    public Lz4FrameDecoder()
    {
        this(false);
    }

    /**
     * Creates a LZ4 decoder with fastest decoder instance available on your machine.
     *
     * @param validateChecksums  if {@code true}, the checksum field will be validated against the actual
     *                           uncompressed data, and if the checksums do not match, a suitable
     *                           {@link DecompressionException} will be thrown
     */
    public Lz4FrameDecoder(boolean validateChecksums)
    {
        this(LZ4Factory.fastestInstance(), validateChecksums);
    }

    /**
     * Creates a new LZ4 decoder with customizable implementation.
     *
     * @param factory            user customizable {@link net.jpountz.lz4.LZ4Factory} instance
     *                           which may be JNI bindings to the original C implementation, a pure Java implementation
     *                           or a Java implementation that uses the {@link sun.misc.Unsafe}
     * @param validateChecksums  if {@code true}, the checksum field will be validated against the actual
     *                           uncompressed data, and if the checksums do not match, a suitable
     *                           {@link DecompressionException} will be thrown. In this case encoder will use
     *                           xxhash hashing for Java, based on Yann Collet's work available at
     *                           <a href="http://code.google.com/p/xxhash/">Google Code</a>.
     */
    public Lz4FrameDecoder(LZ4Factory factory, boolean validateChecksums)
    {
        this(factory, validateChecksums ? new CRC32(): null);
    }

    /**
     * Creates a new customizable LZ4 decoder.
     *
     * @param factory   user customizable {@link net.jpountz.lz4.LZ4Factory} instance
     *                  which may be JNI bindings to the original C implementation, a pure Java implementation
     *                  or a Java implementation that uses the {@link sun.misc.Unsafe}
     * @param checksum  the {@link Checksum} instance to use to check data for integrity.
     *                  You may set {@code null} if you do not want to validate checksum of each block
     */
    public Lz4FrameDecoder(LZ4Factory factory, CRC32 checksum)
    {
        if (factory == null)
            throw new NullPointerException("factory");
        decompressor = factory.fastDecompressor();
        this.checksum = checksum;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
    {
        try
        {
            switch (currentState)
            {
            case INIT_BLOCK:
                if (in.readableBytes() < HEADER_LENGTH)
                    break;

                final long magic = in.readLong();
                if (magic != MAGIC_NUMBER)
                    throw new DecompressionException("unexpected block identifier");

                final int token = in.readByte();
                final int compressionLevel = (token & 0x0F) + COMPRESSION_LEVEL_BASE;
                int blockType = token & 0xF0;

                int compressedLength = Integer.reverseBytes(in.readInt());
                if (compressedLength < 0 || compressedLength > MAX_BLOCK_SIZE)
                {
                    throw new DecompressionException(String.format(
                            "invalid compressedLength: %d (expected: 0-%d)",
                            compressedLength, MAX_BLOCK_SIZE));
                }

                int decompressedLength = Integer.reverseBytes(in.readInt());
                final int maxDecompressedLength = 1 << compressionLevel;
                if (decompressedLength < 0 || decompressedLength > maxDecompressedLength)
                {
                    throw new DecompressionException(String.format(
                            "invalid decompressedLength: %d (expected: 0-%d)",
                            decompressedLength, maxDecompressedLength));
                }
                if (decompressedLength == 0 && compressedLength != 0
                        || decompressedLength != 0 && compressedLength == 0
                        || blockType == BLOCK_TYPE_NON_COMPRESSED && decompressedLength != compressedLength)
                {
                    throw new DecompressionException(String.format(
                            "stream corrupted: compressedLength(%d) and decompressedLength(%d) mismatch",
                            compressedLength, decompressedLength));
                }

                int currentChecksum = Integer.reverseBytes(in.readInt());
                if (decompressedLength == 0 && compressedLength == 0)
                {
                    if (currentChecksum != 0)
                        throw new DecompressionException("stream corrupted: checksum error");

                    // this is the encoder's way of letting us know that the 'session' is complete
                    currentState = State.FINISHED;
                    break;
                }

                this.blockType = blockType;
                this.compressedLength = compressedLength;
                this.decompressedLength = decompressedLength;
                this.currentChecksum = currentChecksum;

                currentState = State.DECOMPRESS_DATA;
            case DECOMPRESS_DATA:
                blockType = this.blockType;
                compressedLength = this.compressedLength;
                decompressedLength = this.decompressedLength;
                currentChecksum = this.currentChecksum;

                if (in.readableBytes() < compressedLength)
                    break;

                final int idx = in.readerIndex();
                ByteBuf uncompressed = null;
                boolean success = false;
                try
                {
                    switch (blockType)
                    {
                    case BLOCK_TYPE_NON_COMPRESSED:
                        uncompressed = in.slice(idx, decompressedLength);
                        uncompressed.retain();
                        in.skipBytes(compressedLength);
                        break;
                    case BLOCK_TYPE_COMPRESSED:
                        try
                        {
                            ByteBuffer nioIn = in.nioBuffer(idx, compressedLength);
                            uncompressed = ctx.alloc().directBuffer(decompressedLength, decompressedLength);
                            ByteBuffer uncompressedBuffer = uncompressed.nioBuffer(0, decompressedLength);
                            decompressor.decompress(nioIn, uncompressedBuffer);
                            if (uncompressedBuffer.position() != decompressedLength)
                                throw new DecompressionException(String.format("stream corrupted: compressedLength(%d) and actual length(%d) mismatch",
                                                                               uncompressedBuffer.position(), decompressedLength));
                            uncompressed.writerIndex(uncompressed.writerIndex() + decompressedLength);
                        }
                        catch (LZ4Exception e)
                        {
                            throw new DecompressionException(e);
                        }
                        finally
                        {
                            in.skipBytes(compressedLength);
                        }
                        break;
                    default:
                        throw new DecompressionException(String.format(
                                "unexpected blockType: %d (expected: %d or %d)",
                                blockType, BLOCK_TYPE_NON_COMPRESSED, BLOCK_TYPE_COMPRESSED));
                    }

                    final CRC32 checksum = this.checksum;
                    if (checksum != null)
                    {
                        checksum.reset();
                        ByteBuffer buffer = uncompressed.nioBuffer();
                        checksum.update(buffer);
                        final int checksumResult = (int) checksum.getValue();
                        if (checksumResult != currentChecksum)
                        {
                            throw new DecompressionException(String.format(
                                    "stream corrupted: mismatching checksum: %d (expected: %d)",
                                    checksumResult, currentChecksum));
                        }
                    }
                    out.add(uncompressed);

                    currentState = State.INIT_BLOCK;
                    success = true;
                }
                finally
                {
                    if (!success && uncompressed != null)
                        uncompressed.release();
                }
                break;
            case FINISHED:
            case CORRUPTED:
                in.skipBytes(in.readableBytes());
                break;
            default:
                throw new IllegalStateException();
            }
        }
        catch (Exception e)
        {
            currentState = State.CORRUPTED;
            throw e;
        }
    }

    /**
     * Returns {@code true} if and only if the end of the compressed stream
     * has been reached.
     */
    public boolean isClosed()
    {
        return currentState == State.FINISHED;
    }
}
