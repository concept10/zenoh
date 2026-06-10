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
package io.zenoh.internal.codec;

import io.netty.buffer.ByteBuf;

/**
 * Variable-length integer (VarInt) encoder/decoder used by the Zenoh wire protocol.
 *
 * <p>Zenoh uses a standard LEB-128-like encoding where:
 * <ul>
 *   <li>Each byte carries 7 data bits (bits 0–6).</li>
 *   <li>Bit 7 (0x80) is the <em>continuation flag</em>: 1 means more bytes follow.</li>
 * </ul>
 *
 * <p>This class also provides helpers for the {@code Zenoh080Bounded} pattern,
 * which bounds a VarInt value to a maximum type width (u8/u16/u32).
 */
public final class VarInt {

    private VarInt() {}

    // -----------------------------------------------------------------------
    // Encoding
    // -----------------------------------------------------------------------

    /**
     * Writes a VarInt-encoded {@code long} into {@code buf}.
     * Consumes 1–9 bytes depending on value magnitude.
     */
    public static void writeU64(ByteBuf buf, long value) {
        long v = value;
        while ((v & ~0x7FL) != 0) {
            buf.writeByte((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        buf.writeByte((int) (v & 0x7F));
    }

    /** Writes a VarInt-encoded {@code int} (treated as unsigned 32-bit). */
    public static void writeU32(ByteBuf buf, int value) {
        writeU64(buf, Integer.toUnsignedLong(value));
    }

    /** Writes a VarInt-encoded {@code int} (bounded to 16 bits). */
    public static void writeU16(ByteBuf buf, int value) {
        writeU64(buf, value & 0xFFFFL);
    }

    /**
     * Writes a length-prefixed byte array using a VarInt bounded to {@code u16}.
     * This corresponds to {@code Zenoh080Bounded<u16>} in the Rust codec.
     */
    public static void writeLenU16(ByteBuf buf, byte[] data) {
        writeU16(buf, data.length);
        buf.writeBytes(data);
    }

    /**
     * Writes a length-prefixed byte array using a VarInt bounded to {@code u32}.
     * This corresponds to {@code Zenoh080Bounded<u32>} in the Rust codec.
     */
    public static void writeLenU32(ByteBuf buf, byte[] data) {
        writeU32(buf, data.length);
        buf.writeBytes(data);
    }

    /**
     * Writes a length-prefixed UTF-8 string using a VarInt bounded to {@code u16}.
     * Used for key expression suffixes and schema strings.
     */
    public static void writeStringU16(ByteBuf buf, String s) {
        byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        writeLenU16(buf, bytes);
    }

    /**
     * Writes a length-prefixed UTF-8 string using a VarInt bounded to {@code u8}.
     * Used for schema fields in Encoding.
     */
    public static void writeStringU8(ByteBuf buf, String s) {
        byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (bytes.length > 0xFF) throw new IllegalArgumentException("String too long for u8 length prefix");
        buf.writeByte(bytes.length);
        buf.writeBytes(bytes);
    }

    // -----------------------------------------------------------------------
    // Decoding
    // -----------------------------------------------------------------------

    /**
     * Reads a VarInt-encoded {@code long} from {@code buf}.
     *
     * @throws IndexOutOfBoundsException if the buffer has too few bytes
     */
    public static long readU64(ByteBuf buf) {
        long result = 0;
        int shift = 0;
        byte b;
        do {
            b = buf.readByte();
            result |= ((long) (b & 0x7F)) << shift;
            shift += 7;
        } while ((b & 0x80) != 0 && shift < 63);
        return result;
    }

    /** Reads a VarInt and returns it as an unsigned 32-bit {@code int}. */
    public static int readU32(ByteBuf buf) {
        return (int) readU64(buf);
    }

    /** Reads a VarInt and returns it as an unsigned 16-bit value. */
    public static int readU16(ByteBuf buf) {
        return (int) (readU64(buf) & 0xFFFFL);
    }

    /**
     * Reads a length-prefixed byte array (length encoded as u16 VarInt).
     * This corresponds to {@code Zenoh080Bounded<u16>} read in the Rust codec.
     */
    public static byte[] readLenU16(ByteBuf buf) {
        int len = readU16(buf);
        byte[] data = new byte[len];
        buf.readBytes(data);
        return data;
    }

    /**
     * Reads a length-prefixed byte array (length encoded as u32 VarInt).
     * This corresponds to {@code Zenoh080Bounded<u32>} read in the Rust codec.
     */
    public static byte[] readLenU32(ByteBuf buf) {
        int len = readU32(buf);
        byte[] data = new byte[len];
        buf.readBytes(data);
        return data;
    }

    /**
     * Reads a length-prefixed UTF-8 string (length encoded as u16 VarInt).
     * Used for key expression suffixes.
     */
    public static String readStringU16(ByteBuf buf) {
        return new String(readLenU16(buf), java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Reads a length-prefixed UTF-8 string (length encoded as u8 raw byte).
     * Used for schema fields in Encoding.
     */
    public static String readStringU8(ByteBuf buf) {
        int len = buf.readUnsignedByte();
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
}
