/*
 * Copyright (c) 2024 ZettaScale Technology
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 *
 * Contributors:
 *   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
 */
package io.zenoh;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.zenoh.internal.codec.VarInt;
import io.zenoh.internal.codec.ZenohDecoder;
import io.zenoh.internal.codec.ZenohEncoder;
import io.zenoh.internal.messages.Messages;
import io.zenoh.internal.messages.Messages.*;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the Zenoh codec (VarInt, ZenohEncoder, ZenohDecoder).
 * No running router is required.
 */
class CodecTest {

    // -----------------------------------------------------------------------
    // VarInt round-trips
    // -----------------------------------------------------------------------

    @Test
    void varintU16RoundTrip() {
        for (int v : new int[]{0, 1, 127, 128, 16383, 65535}) {
            ByteBuf buf = Unpooled.buffer(8);
            VarInt.writeU16(buf, v);
            int read = VarInt.readU16(buf);
            assertEquals(v, read, "u16 round-trip failed for " + v);
            buf.release();
        }
    }

    @Test
    void varintU32RoundTrip() {
        for (long v : new long[]{0, 1, 0x7FL, 0x80L, 0xFFFFFFFFL}) {
            ByteBuf buf = Unpooled.buffer(8);
            VarInt.writeU32(buf, (int) v);
            long read = VarInt.readU32(buf);
            assertEquals(v & 0xFFFFFFFFL, read & 0xFFFFFFFFL, "u32 round-trip failed for " + v);
            buf.release();
        }
    }

    @Test
    void varintU64RoundTrip() {
        for (long v : new long[]{0L, 1L, 127L, 128L, 0x3FFF_FFFF_FFFFL}) {
            ByteBuf buf = Unpooled.buffer(16);
            VarInt.writeU64(buf, v);
            long read = VarInt.readU64(buf);
            assertEquals(v, read, "u64 round-trip failed for " + v);
            buf.release();
        }
    }

    @Test
    void varintStringU16RoundTrip() {
        String s = "hello/world";
        ByteBuf buf = Unpooled.buffer(32);
        VarInt.writeStringU16(buf, s);
        String read = VarInt.readStringU16(buf);
        assertEquals(s, read);
        buf.release();
    }

    @Test
    void varintLenU32RoundTrip() {
        byte[] bytes = "zenoh".getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = Unpooled.buffer(32);
        VarInt.writeLenU32(buf, bytes);
        byte[] read = VarInt.readLenU32(buf);
        assertArrayEquals(bytes, read);
        buf.release();
    }

    // -----------------------------------------------------------------------
    // InitSyn encoding (smoke test for framing)
    // -----------------------------------------------------------------------

    @Test
    void initSynFrameHasTwoBytePrefix() {
        byte[] zid = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
        InitSyn msg = new InitSyn(8, 0x02, zid);
        ByteBuf buf = ZenohEncoder.encodeInitSyn(msg);
        assertTrue(buf.readableBytes() > 2, "Frame must be longer than the 2-byte prefix");
        // first two bytes are the little-endian length
        int lo = buf.readUnsignedByte();
        int hi = buf.readUnsignedByte();
        int len = lo | (hi << 8);
        assertEquals(len, buf.readableBytes(), "2-byte LE prefix must equal remaining bytes");
        buf.release();
    }

    // -----------------------------------------------------------------------
    // Push(Put) encode / Push decode round-trip
    // -----------------------------------------------------------------------

    @Test
    void pushPutRoundTrip() {
        WireExpr wireExpr = new WireExpr(0, "demo/test", false);
        ZBytes payload    = ZBytes.of("hello");
        Put put           = new Put(payload, Encoding.TEXT_PLAIN);
        Push push         = new Push(wireExpr, put);

        // Encode
        ByteBuf frame = ZenohEncoder.encodeFrameWithPush(1L, push, true);
        assertNotNull(frame);
        assertTrue(frame.readableBytes() > 2);

        // Skip 2-byte LE prefix
        int lo = frame.readUnsignedByte();
        int hi = frame.readUnsignedByte();
        int batchLen = lo | (hi << 8);
        assertEquals(batchLen, frame.readableBytes());

        // Transport header byte (ID_FRAME | FLAG_R) = 0x25
        int frameHeader = frame.readUnsignedByte();
        assertEquals(Messages.ID_FRAME | Messages.FLAG_R, frameHeader,
                "Frame header should be ID_FRAME | FLAG_R");

        // Sequence number (should be 1)
        long sn = VarInt.readU64(frame);
        assertEquals(1L, sn);

        // Now the rest is the Push network message
        Object decoded = ZenohDecoder.decodeNetworkMessage(frame);
        assertInstanceOf(Push.class, decoded, "Should decode to a Push");

        Push decodedPush = (Push) decoded;
        assertTrue(decodedPush.isPut());
        assertEquals("demo/test", decodedPush.wireExpr.suffix());
        assertArrayEquals(payload.rawBytes(), decodedPush.put.payload().rawBytes());

        frame.release();
    }

    // -----------------------------------------------------------------------
    // DeclareSubscriber encode sanity check
    // -----------------------------------------------------------------------

    @Test
    void declareSubscriberFrameIsNonEmpty() {
        WireExpr wireExpr = new WireExpr(0, "demo/**", false);
        DeclareSubscriber decl = new DeclareSubscriber(42L, wireExpr);
        ByteBuf frame = ZenohEncoder.encodeFrameWithDeclareSubscriber(2L, decl);
        assertTrue(frame.readableBytes() > 4, "Encoded frame must have content");
        frame.release();
    }

    // -----------------------------------------------------------------------
    // Encoding codec
    // -----------------------------------------------------------------------

    @Test
    void encodingRoundTrip() {
        Encoding enc = Encoding.TEXT_PLAIN;
        ByteBuf buf = Unpooled.buffer(8);
        ZenohEncoder.encodeEncoding(buf, enc);

        // decode: read shifted id
        long idShifted = VarInt.readU32(buf);
        boolean hasSchema = (idShifted & 1) == 1;
        int id = (int) (idShifted >> 1);
        assertEquals(enc.id(), id);
        assertFalse(hasSchema);
        buf.release();
    }
}
