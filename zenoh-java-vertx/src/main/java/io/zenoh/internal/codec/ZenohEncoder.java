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
import io.netty.buffer.Unpooled;
import io.zenoh.Encoding;
import io.zenoh.ZBytes;
import io.zenoh.internal.messages.Messages;
import io.zenoh.internal.messages.Messages.*;

import java.nio.charset.StandardCharsets;

/**
 * Encodes Zenoh protocol messages into {@link ByteBuf}s.
 *
 * <p>Each public method returns a heap-allocated {@link ByteBuf} containing
 * the fully-framed message (including the 2-byte little-endian batch-length prefix
 * required by stream transports such as TCP).
 *
 * <p>The encoding matches the Zenoh 0.8 wire protocol as implemented in the Rust
 * {@code zenoh-codec} crate.
 */
public final class ZenohEncoder {

    private ZenohEncoder() {}

    // -----------------------------------------------------------------------
    // TCP batch framing
    // -----------------------------------------------------------------------

    /**
     * Wraps an encoded message body in the 2-byte little-endian length prefix
     * required by TCP stream transports.
     */
    public static ByteBuf frame(ByteBuf body) {
        int len = body.readableBytes();
        ByteBuf out = Unpooled.buffer(2 + len);
        out.writeByte(len & 0xFF);
        out.writeByte((len >> 8) & 0xFF);
        out.writeBytes(body);
        body.release();
        return out;
    }

    // -----------------------------------------------------------------------
    // Transport layer: Init/Open handshake
    // -----------------------------------------------------------------------

    /**
     * Encodes an {@link InitSyn} (client → router, the first handshake message).
     *
     * <pre>
     * header: ID_INIT | FLAG_S
     * version: u8
     * flags:   ((zid_len - 1) << 4) | whatami
     * zid:     zid_len bytes
     * resolution: u8  (if S)
     * batch_size: u16 LE  (if S)
     * </pre>
     */
    public static ByteBuf encodeInitSyn(InitSyn msg) {
        ByteBuf body = Unpooled.buffer(64);

        // Header: always set the S flag so we negotiate batch size
        int header = Messages.ID_INIT | Messages.FLAG_S;
        body.writeByte(header);

        // Version
        body.writeByte(msg.version());

        // WhatAmI + ZID length byte
        int zidLen = msg.zid().length;
        int flagsByte = ((zidLen - 1) << 4) | (msg.whatami() & 0x03);
        body.writeByte(flagsByte);

        // ZID bytes
        body.writeBytes(msg.zid());

        // Resolution (default: 0x00) + batch size (default: 0xFFFF LE)
        body.writeByte(0x00); // resolution
        body.writeByte(0xFF); // batch_size low byte
        body.writeByte(0xFF); // batch_size high byte

        return frame(body);
    }

    /**
     * Encodes an {@link OpenSyn} (client → router, third handshake message).
     *
     * <pre>
     * header: ID_OPEN (no T flag → lease in ms)
     * lease:  VarInt (ms)
     * initial_sn: VarInt
     * cookie: u16-bounded bytes
     * </pre>
     */
    public static ByteBuf encodeOpenSyn(OpenSyn msg) {
        ByteBuf body = Unpooled.buffer(64 + msg.cookie().length);

        // Header: T=0 means lease is in milliseconds
        body.writeByte(Messages.ID_OPEN);

        // Lease in ms (VarInt)
        VarInt.writeU64(body, msg.leaseMs());

        // Initial sequence number (VarInt)
        VarInt.writeU64(body, msg.initialSn());

        // Cookie (length-prefixed by BatchSize / u16 VarInt)
        VarInt.writeLenU16(body, msg.cookie());

        return frame(body);
    }

    /**
     * Encodes a {@link Close} transport message.
     *
     * <pre>
     * header: ID_CLOSE
     * reason: u8
     * </pre>
     */
    public static ByteBuf encodeClose(Close msg) {
        ByteBuf body = Unpooled.buffer(3);
        body.writeByte(Messages.ID_CLOSE);
        body.writeByte(msg.reason());
        return frame(body);
    }

    /**
     * Encodes a {@link KeepAlive} transport message.
     */
    public static ByteBuf encodeKeepAlive() {
        ByteBuf body = Unpooled.buffer(1);
        body.writeByte(Messages.ID_KEEP_ALIVE);
        return frame(body);
    }

    // -----------------------------------------------------------------------
    // Network layer inside a FRAME
    // -----------------------------------------------------------------------

    /**
     * Encodes a FRAME message containing a single PUSH(PUT) or PUSH(DEL).
     *
     * <pre>
     * Transport frame header: ID_FRAME | FLAG_R  (reliable)
     * SN: VarInt
     * NetworkMessage: PUSH header, wire_expr, extensions, PushBody
     * </pre>
     */
    public static ByteBuf encodeFrameWithPush(long sn, Push push, boolean reliable) {
        ByteBuf netMsg = encodePush(push);
        ByteBuf body   = Unpooled.buffer(16 + netMsg.readableBytes());

        // Frame header
        int frameHeader = Messages.ID_FRAME;
        if (reliable) frameHeader |= Messages.FLAG_R;
        body.writeByte(frameHeader);

        // Sequence number
        VarInt.writeU64(body, sn);

        // Network message payload
        body.writeBytes(netMsg);
        netMsg.release();

        return frame(body);
    }

    /**
     * Encodes a FRAME message containing a Declare(DeclareSubscriber).
     */
    public static ByteBuf encodeFrameWithDeclareSubscriber(long sn, DeclareSubscriber decl) {
        ByteBuf netMsg = encodeDeclareSubscriber(decl);
        return encodeFrameWithNetMsg(sn, netMsg, true);
    }

    /**
     * Encodes a FRAME message containing a Declare(DeclareQueryable).
     */
    public static ByteBuf encodeFrameWithDeclareQueryable(long sn, DeclareQueryable decl) {
        ByteBuf netMsg = encodeDeclareQueryable(decl);
        return encodeFrameWithNetMsg(sn, netMsg, true);
    }

    /**
     * Encodes a FRAME message containing a Declare(UndeclareSubscriber).
     */
    public static ByteBuf encodeFrameWithUndeclareSubscriber(long sn, UndeclareSubscriber decl) {
        ByteBuf netMsg = encodeUndeclareSubscriber(decl);
        return encodeFrameWithNetMsg(sn, netMsg, true);
    }

    /**
     * Encodes a FRAME message containing a Declare(DeclareKeyExpr).
     */
    public static ByteBuf encodeFrameWithDeclareKeyExpr(long sn, DeclareKeyExpr decl) {
        ByteBuf netMsg = encodeDeclareKeyExpr(decl);
        return encodeFrameWithNetMsg(sn, netMsg, true);
    }

    /**
     * Encodes a FRAME message containing an Interest.
     */
    public static ByteBuf encodeFrameWithInterest(long sn, Interest interest) {
        ByteBuf netMsg = encodeInterest(interest);
        return encodeFrameWithNetMsg(sn, netMsg, true);
    }

    /**
     * Encodes a FRAME message containing a Request (for get queries).
     */
    public static ByteBuf encodeFrameWithRequest(long sn, NetRequest request) {
        ByteBuf netMsg = encodeRequest(request);
        return encodeFrameWithNetMsg(sn, netMsg, true);
    }

    /**
     * Encodes a FRAME message containing an UndeclareQueryable.
     */
    public static ByteBuf encodeFrameWithNetMsg(long sn, UndeclareQueryable undecl) {
        ByteBuf netMsg = encodeUndeclareQueryable(undecl);
        return encodeFrameWithNetMsg(sn, netMsg, true);
    }

    /**
     * Encodes a FRAME message containing a Response (reply to a get query).
     */
    public static ByteBuf encodeFrameWithResponse(long sn, NetResponse resp) {
        ByteBuf netMsg = encodeResponse(resp);
        return encodeFrameWithNetMsg(sn, netMsg, true);
    }

    /**
     * Encodes a FRAME message containing a ResponseFinal.
     */
    public static ByteBuf encodeFrameWithResponseFinal(long sn, ResponseFinal rf) {
        ByteBuf netMsg = encodeResponseFinal(rf);
        return encodeFrameWithNetMsg(sn, netMsg, true);
    }

    private static ByteBuf encodeFrameWithNetMsg(long sn, ByteBuf netMsg, boolean reliable) {
        ByteBuf body = Unpooled.buffer(16 + netMsg.readableBytes());
        int frameHeader = Messages.ID_FRAME;
        if (reliable) frameHeader |= Messages.FLAG_R;
        body.writeByte(frameHeader);
        VarInt.writeU64(body, sn);
        body.writeBytes(netMsg);
        netMsg.release();
        return frame(body);
    }

    // -----------------------------------------------------------------------
    // PUSH message
    // -----------------------------------------------------------------------

    private static ByteBuf encodePush(Push push) {
        ByteBuf body = Unpooled.buffer(128);

        // PUSH header
        int pushHeader = Messages.ID_PUSH;
        if (push.wireExpr.suffix() != null && !push.wireExpr.suffix().isEmpty()) {
            pushHeader |= Messages.FLAG_N; // named
        }
        if (push.wireExpr.senderMapping()) {
            pushHeader |= Messages.FLAG_M; // sender mapping
        }
        body.writeByte(pushHeader);

        // WireExpr (scope + optional suffix)
        encodeWireExpr(body, push.wireExpr);

        // PushBody = Put or Del
        if (push.isPut()) {
            encodePut(body, push.put);
        } else {
            encodeDel(body);
        }

        return body;
    }

    private static void encodePut(ByteBuf body, Messages.Put put) {
        // Put header
        int putHeader = Messages.ID_PUT;
        boolean hasEncoding = put.encoding() != null
                && !put.encoding().equals(Encoding.empty());
        if (hasEncoding) {
            putHeader |= Messages.FLAG_E;
        }
        body.writeByte(putHeader);

        // Encoding (optional)
        if (hasEncoding) {
            encodeEncoding(body, put.encoding());
        }

        // Payload (u32-bounded)
        byte[] bytes = put.payload() != null ? put.payload().rawBytes() : new byte[0];
        VarInt.writeLenU32(body, bytes);
    }

    private static void encodeDel(ByteBuf body) {
        body.writeByte(Messages.ID_DEL);
    }

    // -----------------------------------------------------------------------
    // DECLARE messages
    // -----------------------------------------------------------------------

    private static ByteBuf encodeDeclare(int bodyFlag, ByteBuf declBody) {
        ByteBuf body = Unpooled.buffer(8 + declBody.readableBytes());
        body.writeByte(Messages.ID_DECLARE | bodyFlag);
        body.writeBytes(declBody);
        declBody.release();
        return body;
    }

    private static ByteBuf encodeDeclareSubscriber(DeclareSubscriber decl) {
        ByteBuf declBuf = Unpooled.buffer(32);

        // DeclareSubscriber sub-header
        int subHeader = Messages.ID_DECLARE_SUBSCRIBER;
        if (decl.wireExpr().suffix() != null && !decl.wireExpr().suffix().isEmpty()) {
            subHeader |= Messages.FLAG_N;
        }
        if (decl.wireExpr().senderMapping()) {
            subHeader |= Messages.FLAG_M;
        }
        declBuf.writeByte(subHeader);

        // Subscriber ID (VarInt u32)
        VarInt.writeU32(declBuf, (int) decl.subscriberId());

        // WireExpr
        encodeWireExpr(declBuf, decl.wireExpr());

        return encodeDeclare(0, declBuf);
    }

    private static ByteBuf encodeUndeclareSubscriber(UndeclareSubscriber decl) {
        ByteBuf declBuf = Unpooled.buffer(8);
        declBuf.writeByte(Messages.ID_UNDECLARE_SUBSCRIBER);
        VarInt.writeU32(declBuf, (int) decl.subscriberId());
        return encodeDeclare(0, declBuf);
    }

    private static ByteBuf encodeDeclareKeyExpr(DeclareKeyExpr decl) {
        ByteBuf declBuf = Unpooled.buffer(32);

        int subHeader = Messages.ID_DECLARE_KEYEXPR;
        if (decl.wireExpr().suffix() != null && !decl.wireExpr().suffix().isEmpty()) {
            subHeader |= Messages.FLAG_N;
        }
        declBuf.writeByte(subHeader);

        // ExprId (VarInt u16)
        VarInt.writeU16(declBuf, (int) decl.exprId());

        // WireExpr
        encodeWireExpr(declBuf, decl.wireExpr());

        return encodeDeclare(0, declBuf);
    }

    private static ByteBuf encodeDeclareQueryable(DeclareQueryable decl) {
        ByteBuf declBuf = Unpooled.buffer(32);

        int subHeader = Messages.ID_DECLARE_QUERYABLE;
        if (decl.wireExpr().suffix() != null && !decl.wireExpr().suffix().isEmpty()) {
            subHeader |= Messages.FLAG_N;
        }
        if (decl.wireExpr().senderMapping()) {
            subHeader |= Messages.FLAG_M;
        }
        if (decl.complete()) {
            subHeader |= Messages.FLAG_C;
        }
        declBuf.writeByte(subHeader);

        // Queryable ID (VarInt u32)
        VarInt.writeU32(declBuf, (int) decl.queryableId());

        // WireExpr
        encodeWireExpr(declBuf, decl.wireExpr());

        return encodeDeclare(0, declBuf);
    }

    // -----------------------------------------------------------------------
    // INTEREST message
    // -----------------------------------------------------------------------

    private static ByteBuf encodeInterest(Interest interest) {
        ByteBuf body = Unpooled.buffer(32);

        // INTEREST header
        int header = Messages.ID_INTEREST;
        if (interest.wireExpr().suffix() != null && !interest.wireExpr().suffix().isEmpty()) {
            header |= Messages.FLAG_N;
        }
        if (interest.wireExpr().senderMapping()) {
            header |= Messages.FLAG_M;
        }
        body.writeByte(header);

        // Interest ID (u32)
        VarInt.writeU32(body, (int) interest.interestId());

        // Mode + options (single byte: lower 2 bits = mode, upper bits = options)
        int modeOpts = (interest.mode() & 0x03) | ((interest.options() & 0x3F) << 2);
        body.writeByte(modeOpts);

        // WireExpr
        encodeWireExpr(body, interest.wireExpr());

        return body;
    }

    // -----------------------------------------------------------------------
    // REQUEST message (for get queries)
    // -----------------------------------------------------------------------

    private static ByteBuf encodeRequest(NetRequest req) {
        ByteBuf body = Unpooled.buffer(64);

        int header = Messages.ID_REQUEST;
        if (req.wireExpr.suffix() != null && !req.wireExpr.suffix().isEmpty()) {
            header |= Messages.FLAG_N;
        }
        if (req.wireExpr.senderMapping()) {
            header |= Messages.FLAG_M;
        }
        boolean hasTimeout = req.timeoutMs > 0;
        boolean hasPayload = req.payload != null && !req.payload.isEmpty();
        // We add extensions for timeout; set Z flag
        if (hasTimeout) {
            header |= Messages.FLAG_Z;
        }
        body.writeByte(header);

        // Request ID (VarInt u32)
        VarInt.writeU32(body, (int) req.requestId);

        // WireExpr
        encodeWireExpr(body, req.wireExpr);

        // Extensions: timeout (ext_id=0x06, z64 value = ms)
        if (hasTimeout) {
            // ext header: (0x06 << 1) | type_z64(0x01) | more(0) = 0x0D
            body.writeByte(0x0D); // ext id 6, type=z64, no more
            VarInt.writeU64(body, req.timeoutMs);
        }

        // RequestBody = Query
        // Query header: ID_QUERY = 0x03
        int qHeader = Messages.ID_QUERY;
        if (hasPayload) {
            qHeader |= Messages.FLAG_Z; // use extensions for payload
        }
        body.writeByte(qHeader);

        // Consolidation mode (0 = no consolidation) - VarInt
        VarInt.writeU64(body, 0);

        // Payload as extension if present
        if (hasPayload) {
            byte[] payloadBytes = req.payload.rawBytes();
            // ext header: (0x05 << 1) | type_zbuf(0x02) | more(0) = 0x0C
            body.writeByte(0x0C);
            VarInt.writeLenU32(body, payloadBytes);
        }

        return body;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Encodes a {@link WireExpr} into {@code buf}.
     * <pre>
     * scope: VarInt u16
     * suffix: if present → u16-bounded UTF-8 string
     * </pre>
     */
    static void encodeWireExpr(ByteBuf buf, WireExpr expr) {
        VarInt.writeU16(buf, (int) expr.scope());
        if (expr.suffix() != null && !expr.suffix().isEmpty()) {
            VarInt.writeStringU16(buf, expr.suffix());
        }
    }

    /**
     * Encodes an {@link Encoding} into {@code buf}.
     * <pre>
     * id: VarInt u32, where bit 0 = has_schema flag
     * schema: optional u8-bounded string
     * </pre>
     */
    public static void encodeEncoding(ByteBuf buf, Encoding enc) {
        int idShifted = enc.id() << 1;
        boolean hasSchema = enc.schema() != null;
        if (hasSchema) idShifted |= 1; // FLAG_S
        VarInt.writeU32(buf, idShifted);
        if (hasSchema) {
            VarInt.writeStringU8(buf, enc.schema());
        }
    }

    // -----------------------------------------------------------------------
    // UNDECLARE QUERYABLE
    // -----------------------------------------------------------------------

    private static ByteBuf encodeUndeclareQueryable(UndeclareQueryable undecl) {
        ByteBuf declBuf = Unpooled.buffer(8);
        declBuf.writeByte(Messages.ID_UNDECLARE_QUERYABLE);
        VarInt.writeU32(declBuf, (int) undecl.queryableId());
        return encodeDeclare(0, declBuf);
    }

    // -----------------------------------------------------------------------
    // RESPONSE message (reply to get query)
    // -----------------------------------------------------------------------

    private static ByteBuf encodeResponse(NetResponse resp) {
        ByteBuf body = Unpooled.buffer(64);

        int header = Messages.ID_RESPONSE;
        if (resp.wireExpr.suffix() != null && !resp.wireExpr.suffix().isEmpty()) {
            header |= Messages.FLAG_N;
        }
        if (resp.wireExpr.senderMapping()) {
            header |= Messages.FLAG_M;
        }
        body.writeByte(header);

        // Request ID (VarInt u32)
        VarInt.writeU32(body, (int) resp.requestId);

        // WireExpr
        encodeWireExpr(body, resp.wireExpr);

        // ResponseBody: Reply or error
        if (resp.isOk()) {
            // Put-based Reply
            int replyHeader = Messages.ID_REPLY;
            boolean hasEncoding = resp.put.encoding() != null
                    && !resp.put.encoding().equals(Encoding.empty());
            if (hasEncoding) replyHeader |= Messages.FLAG_E;
            body.writeByte(replyHeader);
            if (hasEncoding) {
                encodeEncoding(body, resp.put.encoding());
            }
            byte[] bytes = resp.put.payload() != null ? resp.put.payload().rawBytes() : new byte[0];
            VarInt.writeLenU32(body, bytes);
        } else {
            // Error Reply
            int errHeader = Messages.ID_ERR;
            body.writeByte(errHeader);
            byte[] bytes = resp.error != null ? resp.error.rawBytes() : new byte[0];
            VarInt.writeLenU32(body, bytes);
        }

        return body;
    }

    // -----------------------------------------------------------------------
    // RESPONSE FINAL message
    // -----------------------------------------------------------------------

    private static ByteBuf encodeResponseFinal(ResponseFinal rf) {
        ByteBuf body = Unpooled.buffer(8);
        body.writeByte(Messages.ID_RESPONSE_FINAL);
        VarInt.writeU32(body, (int) rf.requestId());
        return body;
    }
}
