package org.embulk.encoder.lz4;

import static org.junit.Assert.*;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

public class LZ4FrameOutputStreamTest {
  private void showHex(byte[] byteArray) {
    for(int i =0;i<byteArray.length;i++){
      if ((i % 16) == 0) {
        System.out.println();
      }
      System.out.print(String.format("%02x ", byteArray[i]));
    }
  }

  @Test
  public void checkValidFormat() throws Exception {
    byte[] testdata = "aabbccddaabbccddaabbccdd".getBytes();
    byte[] expected = new byte[]{
        (byte)0x04,(byte)0x22,(byte)0x4d,(byte)0x18,
        (byte)0x64,(byte)0x40,(byte)0xa7,
        (byte)0x11,(byte)0x00,(byte)0x00,(byte)0x00,
        (byte)0x87,(byte)0x61,(byte)0x61,(byte)0x62,(byte)0x62,
        (byte)0x63,(byte)0x63,(byte)0x64,(byte)0x64,
        (byte)0x08,(byte)0x00,(byte)0x50,
        (byte)0x62,(byte)0x63,(byte)0x63,(byte)0x64,(byte)0x64,
        (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,
        (byte)0xd2,(byte)0xa3,(byte)0xc6,(byte)0x3e
    };

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    OutputStream lz4 = new LZ4FrameOutputStream(output, 64*1024);

    lz4.write(testdata[0]);
    lz4.write(testdata[1]);
    lz4.write(testdata[2]);
    lz4.write(testdata[3]);
    lz4.write(testdata, 4, testdata.length - 4);
    lz4.close();

    byte[] result = output.toByteArray();
    showHex(result);

    assertEquals(expected.length, result.length);
    for(int i = 0; i < expected.length; i++){
      assertEquals(expected[i], result[i]);
    }
  }

  @Test(expected = Exception.class)
  public void checkWriteAfterClosed() throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    OutputStream lz4 = new LZ4FrameOutputStream(output);

    lz4.write(0x00);
    lz4.close();
    lz4.write(0x01);
  }

}