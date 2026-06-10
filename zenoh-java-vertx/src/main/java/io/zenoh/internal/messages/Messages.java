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
package io.zenoh.internal.messages;

import io.zenoh.Encoding;
import io.zenoh.KeyExpr;
import io.zenoh.SampleKind;
import io.zenoh.ZBytes;

/**
 * Wire-level message types for the Zenoh 0.8 transport and network layers.
 *
 * <p>This class is a namespace containing all the inner message record types
 * used during encoding/decoding. It mirrors the Rust protocol types from
 * {@code zenoh-protocol}.
 *
 * <h3>Protocol overview</h3>
 * <pre>
 * TCP framing:      [ u16_le length ][ message bytes... ]
 *
 * Transport layer:
 *   INIT  (0x01)   – InitSyn / InitAck handshake
 *   OPEN  (0x02)   – OpenSyn / OpenAck handshake
 *   CLOSE (0x03)   – close the transport
 *   KEEP_ALIVE (0x04)
 *   FRAME (0x05)   – contains one or more NetworkMessages
 *
 * Network layer (inside Frame payload):
 *   INTEREST  (0x19)
 *   RESPONSE_FINAL (0x1a)
 *   RESPONSE  (0x1b)
 *   REQUEST   (0x1c)
 *   PUSH      (0x1d)  – carries Put or Del
 *   DECLARE   (0x1e)  – subscriber/queryable/keyexpr declarations
 *
 * Zenoh layer (inside Push/Request/Response):
 *   PUT   (0x01)
 *   DEL   (0x02)
 *   QUERY (0x03)
 *   REPLY (0x04)
 * </pre>
 */
public final class Messages {

    private Messages() {}

    // -----------------------------------------------------------------------
    // Transport-layer IDs (lower 5 bits of header byte)
    // -----------------------------------------------------------------------
    public static final int ID_OAM        = 0x00;
    public static final int ID_INIT       = 0x01;
    public static final int ID_OPEN       = 0x02;
    public static final int ID_CLOSE      = 0x03;
    public static final int ID_KEEP_ALIVE = 0x04;
    public static final int ID_FRAME      = 0x05;
    public static final int ID_FRAGMENT   = 0x06;
    public static final int ID_JOIN       = 0x07;

    // -----------------------------------------------------------------------
    // Network-layer IDs (lower 5 bits of header byte, inside a Frame)
    // -----------------------------------------------------------------------
    public static final int ID_INTEREST       = 0x19;
    public static final int ID_RESPONSE_FINAL = 0x1a;
    public static final int ID_RESPONSE       = 0x1b;
    public static final int ID_REQUEST        = 0x1c;
    public static final int ID_PUSH           = 0x1d;
    public static final int ID_DECLARE        = 0x1e;

    // -----------------------------------------------------------------------
    // Zenoh-layer IDs (inside Put/Del/Query/Reply bodies)
    // -----------------------------------------------------------------------
    public static final int ID_PUT   = 0x01;
    public static final int ID_DEL   = 0x02;
    public static final int ID_QUERY = 0x03;
    public static final int ID_REPLY = 0x04;
    public static final int ID_ERR   = 0x05;

    // -----------------------------------------------------------------------
    // DeclareBody sub-IDs
    // -----------------------------------------------------------------------
    public static final int ID_DECLARE_KEYEXPR       = 0x00;
    public static final int ID_UNDECLARE_KEYEXPR     = 0x01;
    public static final int ID_DECLARE_SUBSCRIBER    = 0x02;
    public static final int ID_UNDECLARE_SUBSCRIBER  = 0x03;
    public static final int ID_DECLARE_QUERYABLE     = 0x04;
    public static final int ID_UNDECLARE_QUERYABLE   = 0x05;
    public static final int ID_DECLARE_FINAL         = 0x1a;

    // -----------------------------------------------------------------------
    // Header flag masks
    // -----------------------------------------------------------------------
    /** Extracts the 5-bit message ID from a header byte. */
    public static int mid(int header) {
        return header & 0x1F;
    }

    /** Tests a specific flag bit in a header byte. */
    public static boolean hasFlag(int header, int flag) {
        return (header & flag) != 0;
    }

    public static final int FLAG_Z = 0x80;  // Extensions follow
    public static final int FLAG_A = 0x20;  // Ack (INIT/OPEN)
    public static final int FLAG_S = 0x40;  // Size params (INIT)
    public static final int FLAG_T = 0x40;  // Lease period in seconds (OPEN)
    public static final int FLAG_R = 0x20;  // Reliable (FRAME)
    public static final int FLAG_N = 0x20;  // Named – key expr has suffix
    public static final int FLAG_M = 0x40;  // Mapping – sender-side mapping
    public static final int FLAG_I = 0x20;  // Interest (DECLARE)
    public static final int FLAG_E = 0x40;  // Encoding present (PUT) – bit 6
    public static final int FLAG_TMS = 0x20; // Timestamp present (PUT) – bit 5
    public static final int FLAG_C = 0x40;  // Complete (QUERYABLE)

    // -----------------------------------------------------------------------
    // Protocol constants
    // -----------------------------------------------------------------------
    public static final int ZENOH_VERSION   = 8;
    public static final int WHATAMI_ROUTER  = 0x00;
    public static final int WHATAMI_PEER    = 0x01;
    public static final int WHATAMI_CLIENT  = 0x02;
    public static final int DEFAULT_BATCH_SIZE = 0xFFFF;

    // -----------------------------------------------------------------------
    // Message records
    // -----------------------------------------------------------------------

    /** The client side of the INIT handshake (InitSyn). */
    public record InitSyn(
            int version,
            int whatami,
            byte[] zid
    ) {}

    /** The router/peer side of the INIT handshake (InitAck). */
    public record InitAck(
            int version,
            int whatami,
            byte[] zid,
            int resolution,
            int batchSize,
            byte[] cookie
    ) {}

    /** The client side of the OPEN handshake (OpenSyn). */
    public record OpenSyn(
            long leaseMs,
            long initialSn,
            byte[] cookie
    ) {}

    /** The router/peer side of the OPEN handshake (OpenAck). */
    public record OpenAck(
            long leaseMs,
            long initialSn
    ) {}

    /** A CLOSE transport message. */
    public record Close(int reason) {}

    /** A KEEP_ALIVE transport message. */
    public record KeepAlive() {}

    /**
     * A wire expression carrying an optional numeric scope ID and an optional
     * string suffix (the human-readable portion of the key expression).
     */
    public record WireExpr(long scope, String suffix, boolean senderMapping) {
        public KeyExpr toKeyExpr() {
            if (scope == 0 && suffix != null && !suffix.isEmpty()) {
                return KeyExpr.of(suffix);
            }
            // When scope != 0 the caller resolves it from the resource table
            return suffix != null && !suffix.isEmpty()
                    ? KeyExpr.of(suffix)
                    : KeyExpr.of(String.valueOf(scope));
        }
    }

    // --- Zenoh-layer payloads -----------------------------------------------

    /** A PUT payload (inside a Push message). */
    public record Put(ZBytes payload, Encoding encoding) {}

    /** A DELETE payload (inside a Push message). */
    public record Del() {}

    // --- Network-layer messages ---------------------------------------------

    /**
     * A PUSH network message containing either a Put or a Del.
     */
    public static final class Push {
        public final WireExpr wireExpr;
        public final Put put;    // null when del is set
        public final Del del;    // null when put is set

        public Push(WireExpr wireExpr, Put put) {
            this.wireExpr = wireExpr;
            this.put = put;
            this.del = null;
        }

        public Push(WireExpr wireExpr, Del del) {
            this.wireExpr = wireExpr;
            this.put = null;
            this.del = del;
        }

        public boolean isPut() { return put != null; }

        public SampleKind kind() {
            return put != null ? SampleKind.PUT : SampleKind.DELETE;
        }
    }

    /** A DECLARE network message (DeclareSubscriber sent by the client). */
    public record DeclareSubscriber(long subscriberId, WireExpr wireExpr) {}

    /** An UNDECLARE SUBSCRIBER network message. */
    public record UndeclareSubscriber(long subscriberId) {}

    /** A DECLARE KEY_EXPR network message (resource table declaration). */
    public record DeclareKeyExpr(long exprId, WireExpr wireExpr) {}

    /** A DECLARE QUERYABLE network message. */
    public record DeclareQueryable(long queryableId, WireExpr wireExpr, boolean complete) {}

    /** An UNDECLARE QUERYABLE network message. */
    public record UndeclareQueryable(long queryableId) {}

    /** An Interest network message sent before subscribing. */
    public record Interest(
            long interestId,
            int mode,            // 0=current, 1=future, 2=currentFuture, 3=final
            int options,         // bitmask: subscribers=1, queryables=2, tokens=4, keyExprs=8
            WireExpr wireExpr
    ) {
        public static final int MODE_CURRENT        = 0;
        public static final int MODE_FUTURE         = 1;
        public static final int MODE_CURRENT_FUTURE = 2;
        public static final int MODE_FINAL          = 3;
        public static final int OPT_SUBSCRIBERS     = 0x01;
        public static final int OPT_QUERYABLES      = 0x02;
        public static final int OPT_TOKENS          = 0x04;
        public static final int OPT_KEY_EXPRS       = 0x08;
        public static final int OPT_RESTRICTED      = 0x20;
    }

    /** A REQUEST network message (for get queries). */
    public static final class NetRequest {
        public final long requestId;
        public final WireExpr wireExpr;
        public final ZBytes payload;
        public final long timeoutMs;

        public NetRequest(long requestId, WireExpr wireExpr, ZBytes payload, long timeoutMs) {
            this.requestId = requestId;
            this.wireExpr  = wireExpr;
            this.payload   = payload;
            this.timeoutMs = timeoutMs;
        }
    }

    /** A RESPONSE network message (reply to a get query). */
    public static final class NetResponse {
        public final long requestId;
        public final WireExpr wireExpr;
        public final Put put;      // null if error
        public final ZBytes error; // null if put

        public NetResponse(long requestId, WireExpr wireExpr, Put put) {
            this.requestId = requestId;
            this.wireExpr  = wireExpr;
            this.put       = put;
            this.error     = null;
        }

        public NetResponse(long requestId, WireExpr wireExpr, ZBytes error) {
            this.requestId = requestId;
            this.wireExpr  = wireExpr;
            this.put       = null;
            this.error     = error;
        }

        public boolean isOk() { return put != null; }
    }

    /** A RESPONSE_FINAL network message (end of responses for a get query). */
    public record ResponseFinal(long requestId) {}
}
