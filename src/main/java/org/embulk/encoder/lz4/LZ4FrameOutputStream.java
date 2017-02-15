package org.embulk.encoder.lz4;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by yuuzi on 17/02/15.
 */
public class LZ4FrameOutputStream extends FilterOutputStream {
  private int blockSize;

  private boolean isFrameStarted = false;
  private boolean isFrameEnded = false;

  private LZ4Compressor compressor;
  private XXHash32 hasher32;
  private StreamingXXHash32 contentChecksummer;
  private XXHash64 hasher64;
  private byte[] compressingData;
  private int compressingDataOffset = 0;
  private boolean isSynchronize = false;

  public LZ4FrameOutputStream(OutputStream outputStream) {
    this(outputStream, 4 * 1024 * 1024);
  }

  public LZ4FrameOutputStream(OutputStream outputStream, int blockSize) {
    this(outputStream,blockSize,false);
  }

  public LZ4FrameOutputStream(OutputStream outputStream, int blockSize, boolean synchronize) {
    super(outputStream);
    if (blockSize != 64 * 1024
        && blockSize != 256 * 1024
        && blockSize != 1024 * 1024
        && blockSize != 4 * 1024 * 1024) {
      throw new IllegalArgumentException("LZ4 Block size is supported 4KiB, 256KiB, 1MiB or 4MiB only.");
    }
    this.blockSize = blockSize;
    this.compressingData = new byte[this.blockSize];

    this.isSynchronize = synchronize;

    this.compressor = LZ4Factory.fastestInstance().fastCompressor();
    this.hasher32 = XXHashFactory.fastestInstance().hash32();
    this.contentChecksummer = XXHashFactory.fastestInstance().newStreamingHash32(0);
  }

  @Override
  public void write(int i) throws IOException {
    if (isFrameEnded){
      throw new IllegalStateException("This stream is already closed.");
    }
    this.compressingData[this.compressingDataOffset++] = (byte) i;

    if (this.compressingDataOffset >= this.blockSize) {
      this.flushBlock();
    }
  }

  @Override
  public void write(byte[] bytes) throws IOException {
    this.write(bytes, 0, bytes.length);
  }

  @Override
  public void write(byte[] bytes, int idx, int len) throws IOException {
    if (isFrameEnded){
      throw new IllegalStateException("This stream is already closed.");
    }

    while (this.blockSize - this.compressingDataOffset < len) {
      final int writable = this.blockSize - this.compressingDataOffset;
      System.arraycopy(bytes, idx, this.compressingData, this.compressingDataOffset, writable);
      this.compressingDataOffset = this.blockSize;
      this.flushBlock();
      len -= writable;
      idx += writable;
    }
    System.arraycopy(bytes, idx, this.compressingData, this.compressingDataOffset, len);
    this.compressingDataOffset += len;

    if (this.compressingDataOffset >= this.blockSize) {
      this.flushBlock();
    }
  }

  @Override
  public void flush() throws IOException {
    if (isSynchronize) {
      this.flushBlock();
    }
    super.flush();
  }

  @Override
  public void close() throws IOException {
    if (!isFrameEnded) {
      this.flushBlock();

      final byte[] footer = new byte[8];
      final int contentChecksum = contentChecksummer.getValue();
      footer[4] = (byte)(contentChecksum & 0x000000ff);
      footer[5] = (byte)((contentChecksum & 0x0000ff00) >> 8);
      footer[6] = (byte)((contentChecksum & 0x00ff0000) >> 16);
      footer[7] = (byte)((contentChecksum & 0xff000000) >> 24);

      this.out.write(footer);

      isFrameEnded = true;
    }
    super.close();
  }

  private void flushBlock() throws IOException {
    if (this.compressingDataOffset > 0) {
      if (!isFrameStarted) {

        //TODO: Support Frame header with Content Size
        // with B.Indep, without B.Checksum, without C.Size, with C.Checksum
        final byte[] frameHeader = new byte[]{0x04, 0x22, 0x4d, 0x18, 0x64, 0x00, 0x00};

        switch (this.blockSize) {
          case 64 * 1024:
            frameHeader[5] = 0x40;
            break;
          case 256 * 1024:
            frameHeader[5] = 0x50;
            break;
          case 1024 * 1024:
            frameHeader[5] = 0x60;
            break;
          case 4096 * 1024:
            frameHeader[5] = 0x70;
            break;
          default:
            throw new IllegalAccessError("Illegal Block Size");
        }
        // TODO: Hash value is definite for very limted variation, so we can pre-calc.
        frameHeader[6] = (byte) ((hasher32.hash(frameHeader, 4, 2, 0) >> 8) & 0xff);

        this.out.write(frameHeader);
        isFrameStarted = true;
      }

      contentChecksummer.update(this.compressingData, 0, this.compressingDataOffset);

      final byte[] compressedSize = new byte[4];
      final byte[] compressed = compressor.compress(
          this.compressingData, 0, this.compressingDataOffset
      );
      compressedSize[0] = (byte)(compressed.length & 0x000000ff);
      compressedSize[1] = (byte)((compressed.length & 0x0000ff00)>>8);
      compressedSize[2] = (byte)((compressed.length & 0x00ff0000)>>16);
      compressedSize[3] = (byte)((compressed.length & 0xff000000)>>24);
      this.out.write(compressedSize);
      this.out.write(compressed);
      //TODO: B.Checksum

      this.compressingDataOffset = 0;
    }
  }
}
