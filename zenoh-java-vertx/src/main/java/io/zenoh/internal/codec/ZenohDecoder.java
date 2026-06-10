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
import io.zenoh.Encoding;
import io.zenoh.ZBytes;
import io.zenoh.internal.messages.Messages;
import io.zenoh.internal.messages.Messages.*;

/**
 * Decodes Zenoh protocol messages from a {@link ByteBuf}.
 *
 * <p>All {@code decode*} methods assume the 2-byte batch-length prefix has
 * already been consumed and the reader index is positioned at the first byte
 * of the transport or network message.
 *
 * <p>The decoding logic mirrors the Rust {@code zenoh-codec} crate for
 * protocol version 0.8.
 */
public final class ZenohDecoder {

    private ZenohDecoder() {}

    // -----------------------------------------------------------------------
    // Dispatch: read one transport-layer message from a batch buffer
    // -----------------------------------------------------------------------

    /**
     * Reads one transport message from {@code buf}.
     *
     * @return one of {@link InitSyn}, {@link InitAck}, {@link OpenSyn},
     *         {@link OpenAck}, {@link Close}, {@link KeepAlive}, or
     *         a {@link DecodedFrame} holding decoded network messages.
     *         Returns {@code null} if the header is unrecognised.
     */
    public static Object decodeTransportMessage(ByteBuf buf) {
        if (!buf.isReadable()) return null;
        int header = buf.readUnsignedByte();
        int id = Messages.mid(header);

        return switch (id) {
            case Messages.ID_INIT       -> decodeInit(header, buf);
            case Messages.ID_OPEN       -> decodeOpen(header, buf);
            case Messages.ID_CLOSE      -> decodeClose(buf);
            case Messages.ID_KEEP_ALIVE -> new KeepAlive();
            case Messages.ID_FRAME      -> decodeFrame(header, buf);
            case Messages.ID_FRAGMENT   -> { skipExtensions(header, buf); yield null; } // not supported
            default                     -> { skipUnknown(header, buf); yield null; }
        };
    }

    // -----------------------------------------------------------------------
    // InitAck
    // -----------------------------------------------------------------------

    private static Object decodeInit(int header, ByteBuf buf) {
        boolean isAck = Messages.hasFlag(header, Messages.FLAG_A);
        boolean hasSizeParams = Messages.hasFlag(header, Messages.FLAG_S);

        int version  = buf.readUnsignedByte();
        int flags    = buf.readUnsignedByte();
        int whatami  = flags & 0x03;
        int zidLen   = 1 + ((flags >> 4) & 0x0F);
        byte[] zid   = new byte[zidLen];
        buf.readBytes(zid);

        int resolution = 0;
        int batchSize  = Messages.DEFAULT_BATCH_SIZE;
        if (hasSizeParams) {
            resolution = buf.readUnsignedByte();
            int bsLow  = buf.readUnsignedByte();
            int bsHigh = buf.readUnsignedByte();
            batchSize  = bsLow | (bsHigh << 8);
        }

        byte[] cookie = new byte[0];
        if (isAck) {
            // Read cookie bounded by BatchSize (u16 VarInt)
            cookie = VarInt.readLenU16(buf);
        }

        skipExtensions(header, buf);

        if (isAck) {
            return new InitAck(version, whatami, zid, resolution, batchSize, cookie);
        } else {
            return new InitSyn(version, whatami, zid);
        }
    }

    // -----------------------------------------------------------------------
    // OpenAck
    // -----------------------------------------------------------------------

    private static Object decodeOpen(int header, ByteBuf buf) {
        boolean isAck        = Messages.hasFlag(header, Messages.FLAG_A);
        boolean leaseSeconds = Messages.hasFlag(header, Messages.FLAG_T);

        long lease = VarInt.readU64(buf);
        if (leaseSeconds) {
            lease = lease * 1000; // convert to ms
        }
        long initialSn = VarInt.readU64(buf);

        byte[] cookie = new byte[0];
        if (!isAck) {
            // OpenSyn carries the cookie
            cookie = VarInt.readLenU16(buf);
        }

        skipExtensions(header, buf);

        if (isAck) {
            return new OpenAck(lease, initialSn);
        } else {
            return new OpenSyn(lease, initialSn, cookie);
        }
    }

    // -----------------------------------------------------------------------
    // Close
    // -----------------------------------------------------------------------

    private static Object decodeClose(ByteBuf buf) {
        int reason = buf.isReadable() ? buf.readUnsignedByte() : 0;
        return new Close(reason);
    }

    // -----------------------------------------------------------------------
    // Frame (contains zero or more NetworkMessages)
    // -----------------------------------------------------------------------

    /**
     * A decoded FRAME holding the parsed network messages.
     */
    public record DecodedFrame(boolean reliable, long sn, java.util.List<Object> messages) {}

    private static DecodedFrame decodeFrame(int frameHeader, ByteBuf buf) {
        boolean reliable = Messages.hasFlag(frameHeader, Messages.FLAG_R);
        long sn = VarInt.readU64(buf);

        // Skip frame extensions if any
        skipExtensions(frameHeader, buf);

        java.util.List<Object> messages = new java.util.ArrayList<>();
        while (buf.isReadable()) {
            int savedIndex = buf.readerIndex();
            try {
                Object msg = decodeNetworkMessage(buf);
                if (msg != null) {
                    messages.add(msg);
                }
            } catch (Exception e) {
                // If we cannot parse a network message, rewind and stop
                buf.readerIndex(savedIndex);
                break;
            }
        }
        return new DecodedFrame(reliable, sn, messages);
    }

    // -----------------------------------------------------------------------
    // Network messages (inside a Frame)
    // -----------------------------------------------------------------------

    /**
     * Reads one network-layer message from {@code buf}.
     *
     * @return one of {@link Push}, {@link DeclareSubscriber},
     *         {@link DeclareKeyExpr}, {@link DeclareQueryable},
     *         {@link NetResponse}, {@link ResponseFinal}, or {@code null}.
     */
    public static Object decodeNetworkMessage(ByteBuf buf) {
        if (!buf.isReadable()) return null;
        int header = buf.readUnsignedByte();
        int id = Messages.mid(header);

        return switch (id) {
            case Messages.ID_PUSH           -> decodePush(header, buf);
            case Messages.ID_DECLARE        -> decodeDeclare(header, buf);
            case Messages.ID_RESPONSE       -> decodeResponse(header, buf);
            case Messages.ID_RESPONSE_FINAL -> decodeResponseFinal(buf);
            case Messages.ID_REQUEST        -> { skipNetRequest(header, buf); yield null; }
            case Messages.ID_INTEREST       -> { skipInterest(header, buf); yield null; }
            default                         -> { skipUnknown(header, buf); yield null; }
        };
    }

    // -----------------------------------------------------------------------
    // PUSH
    // -----------------------------------------------------------------------

    private static Push decodePush(int header, ByteBuf buf) {
        boolean hasName   = Messages.hasFlag(header, Messages.FLAG_N);
        boolean hasMapping = Messages.hasFlag(header, Messages.FLAG_M);

        WireExpr wireExpr = decodeWireExpr(buf, hasName, hasMapping);

        skipExtensions(header, buf);

        // Decode PushBody (Put or Del)
        int pushBodyHeader = buf.readUnsignedByte();
        int pushBodyId = Messages.mid(pushBodyHeader);

        if (pushBodyId == Messages.ID_PUT) {
            Messages.Put put = decodePut(pushBodyHeader, buf);
            return new Push(wireExpr, put);
        } else if (pushBodyId == Messages.ID_DEL) {
            skipExtensions(pushBodyHeader, buf);
            return new Push(wireExpr, new Del());
        } else {
            // Unknown push body — skip remainder
            buf.skipBytes(buf.readableBytes());
            return null;
        }
    }

    private static Messages.Put decodePut(int header, ByteBuf buf) {
        boolean hasTimestamp = Messages.hasFlag(header, Messages.FLAG_TMS);
        boolean hasEncoding  = Messages.hasFlag(header, Messages.FLAG_E);

        if (hasTimestamp) {
            // Skip 8-byte NTP64 + up to 16-byte ID (uhlc timestamp)
            skipTimestamp(buf);
        }

        Encoding encoding = Encoding.empty();
        if (hasEncoding) {
            encoding = decodeEncoding(buf);
        }

        skipExtensions(header, buf);

        // Payload: u32-bounded
        byte[] payloadBytes = VarInt.readLenU32(buf);
        return new Messages.Put(ZBytes.of(payloadBytes), encoding);
    }

    // -----------------------------------------------------------------------
    // DECLARE
    // -----------------------------------------------------------------------

    private static Object decodeDeclare(int header, ByteBuf buf) {
        boolean hasInterestId = Messages.hasFlag(header, Messages.FLAG_I);

        long interestId = 0;
        if (hasInterestId) {
            interestId = VarInt.readU64(buf);
        }

        skipExtensions(header, buf);

        // Decode DeclareBody
        if (!buf.isReadable()) return null;
        int declHeader = buf.readUnsignedByte();
        int declId = Messages.mid(declHeader);

        return switch (declId) {
            case Messages.ID_DECLARE_SUBSCRIBER -> {
                boolean hasName    = Messages.hasFlag(declHeader, Messages.FLAG_N);
                boolean hasMapping = Messages.hasFlag(declHeader, Messages.FLAG_M);
                long subId = VarInt.readU64(buf);
                WireExpr wireExpr = decodeWireExpr(buf, hasName, hasMapping);
                skipExtensions(declHeader, buf);
                yield new DeclareSubscriber(subId, wireExpr);
            }
            case Messages.ID_DECLARE_KEYEXPR -> {
                boolean hasName = Messages.hasFlag(declHeader, Messages.FLAG_N);
                long exprId = VarInt.readU64(buf);
                WireExpr wireExpr = decodeWireExpr(buf, hasName, false);
                skipExtensions(declHeader, buf);
                yield new DeclareKeyExpr(exprId, wireExpr);
            }
            case Messages.ID_DECLARE_QUERYABLE -> {
                boolean hasName    = Messages.hasFlag(declHeader, Messages.FLAG_N);
                boolean hasMapping = Messages.hasFlag(declHeader, Messages.FLAG_M);
                boolean complete   = Messages.hasFlag(declHeader, Messages.FLAG_C);
                long qId = VarInt.readU64(buf);
                WireExpr wireExpr = decodeWireExpr(buf, hasName, hasMapping);
                skipExtensions(declHeader, buf);
                yield new DeclareQueryable(qId, wireExpr, complete);
            }
            case Messages.ID_DECLARE_FINAL -> {
                skipExtensions(declHeader, buf);
                yield null; // DeclareFinal – informational only
            }
            default -> {
                skipUnknown(declHeader, buf);
                yield null;
            }
        };
    }

    // -----------------------------------------------------------------------
    // RESPONSE
    // -----------------------------------------------------------------------

    private static Object decodeResponse(int header, ByteBuf buf) {
        boolean hasName    = Messages.hasFlag(header, Messages.FLAG_N);
        boolean hasMapping = Messages.hasFlag(header, Messages.FLAG_M);

        long requestId = VarInt.readU64(buf);
        WireExpr wireExpr = decodeWireExpr(buf, hasName, hasMapping);

        skipExtensions(header, buf);

        // ResponseBody = Reply | Err
        if (!buf.isReadable()) return null;
        int replyHeader = buf.readUnsignedByte();
        int replyId = Messages.mid(replyHeader);

        if (replyId == Messages.ID_REPLY) {
            // Reply = Put
            Messages.Put put = decodePut(replyHeader, buf);
            return new NetResponse(requestId, wireExpr, put);
        } else if (replyId == Messages.ID_DEL) {
            // Err
            skipExtensions(replyHeader, buf);
            byte[] errBytes = VarInt.readLenU32(buf);
            return new NetResponse(requestId, wireExpr, ZBytes.of(errBytes));
        } else {
            skipUnknown(replyHeader, buf);
            return null;
        }
    }

    private static ResponseFinal decodeResponseFinal(ByteBuf buf) {
        long requestId = VarInt.readU64(buf);
        return new ResponseFinal(requestId);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Decodes a {@link WireExpr} from {@code buf}.
     *
     * <pre>
     * scope: u16 VarInt
     * suffix: optional u16-bounded UTF-8 string (if {@code hasName})
     * </pre>
     */
    static WireExpr decodeWireExpr(ByteBuf buf, boolean hasName, boolean senderMapping) {
        long scope = VarInt.readU16(buf);
        String suffix = "";
        if (hasName) {
            suffix = VarInt.readStringU16(buf);
        }
        return new WireExpr(scope, suffix, senderMapping);
    }

    /**
     * Decodes an {@link Encoding} from {@code buf}.
     * <pre>
     * id: u32 VarInt where bit 0 = has_schema flag
     * schema: optional u8-bounded UTF-8 string
     * </pre>
     */
    static Encoding decodeEncoding(ByteBuf buf) {
        long idRaw = VarInt.readU64(buf);
        boolean hasSchema = (idRaw & 1) != 0;
        int id = (int) (idRaw >> 1);
        String schema = null;
        if (hasSchema) {
            schema = VarInt.readStringU8(buf);
        }
        return new Encoding(id, schema);
    }

    /**
     * Skips a uhlc timestamp (8-byte NTP64 value + variable-length ID).
     */
    private static void skipTimestamp(ByteBuf buf) {
        // NTP64 is 8 bytes
        if (buf.readableBytes() >= 8) {
            buf.skipBytes(8);
        }
        // ID: first byte encodes length
        if (buf.isReadable()) {
            int idLen = buf.readUnsignedByte();
            if (buf.readableBytes() >= idLen) {
                buf.skipBytes(idLen);
            }
        }
    }

    /**
     * Skips all extensions following a header with the Z flag set.
     * Each extension has a header byte:
     * <pre>
     *   bits[6:5] = type (00=unit, 01=z64, 10=zbuf)
     *   bit[7]    = more extensions follow
     * </pre>
     */
    static void skipExtensions(int header, ByteBuf buf) {
        if (!Messages.hasFlag(header, Messages.FLAG_Z)) return;
        boolean more = true;
        while (more && buf.isReadable()) {
            int extHeader = buf.readUnsignedByte();
            more = (extHeader & 0x80) != 0;
            int extType = (extHeader >> 5) & 0x03;
            switch (extType) {
                case 0x00 -> {} // unit: no body
                case 0x01 -> VarInt.readU64(buf); // z64: one VarInt
                case 0x02 -> { // zbuf: length-prefixed bytes
                    byte[] b = VarInt.readLenU32(buf);
                    // discard
                }
                default -> buf.skipBytes(buf.readableBytes()); // unknown; stop
            }
        }
    }

    private static void skipUnknown(int header, ByteBuf buf) {
        // best-effort: if Z flag, skip extensions; then consume remainder of batch
        skipExtensions(header, buf);
        // Remaining bytes in this batch are consumed by caller
    }

    private static void skipNetRequest(int header, ByteBuf buf) {
        // Skip a REQUEST network message
        skipExtensions(header, buf);
    }

    private static void skipInterest(int header, ByteBuf buf) {
        skipExtensions(header, buf);
    }
}
