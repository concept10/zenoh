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

import io.vertx.core.Future;

/**
 * An incoming query received by a {@link Queryable}.
 *
 * <p>A queryable handler must call either {@link #reply(KeyExpr, ZBytes)} or
 * {@link #replyError(ZBytes)} to send a response back to the querier.
 * Calling neither results in a timeout on the querier side.
 */
public final class Query {

    private final long requestId;
    private final KeyExpr keyExpr;
    private final ZBytes payload;
    private final Encoding encoding;
    private final Session session;
    private volatile boolean replied = false;

    Query(long requestId, KeyExpr keyExpr, ZBytes payload, Encoding encoding, Session session) {
        this.requestId = requestId;
        this.keyExpr   = keyExpr;
        this.payload   = payload;
        this.encoding  = encoding;
        this.session   = session;
    }

    /** The key expression matched by this query. */
    public KeyExpr keyExpr() {
        return keyExpr;
    }

    /** The optional payload of this query (may be empty). */
    public ZBytes payload() {
        return payload;
    }

    /** The encoding of the query payload. */
    public Encoding encoding() {
        return encoding;
    }

    /**
     * Sends a successful reply to the querier.
     *
     * @param keyExpr the key expression of the reply sample
     * @param payload the reply payload
     */
    public void reply(KeyExpr keyExpr, ZBytes payload) {
        reply(keyExpr, payload, Encoding.ZENOH_BYTES);
    }

    /**
     * Sends a successful reply with encoding metadata.
     */
    public void reply(KeyExpr keyExpr, ZBytes payload, Encoding encoding) {
        requireNotReplied();
        replied = true;
        session.sendReply(requestId, keyExpr, payload, encoding);
    }

    /**
     * Sends a successful reply from a string payload (UTF-8, TEXT_PLAIN encoding).
     */
    public void reply(KeyExpr keyExpr, String payload) {
        reply(keyExpr, ZBytes.of(payload), Encoding.TEXT_PLAIN);
    }

    /**
     * Sends an error reply to the querier.
     *
     * @param error the error payload
     */
    public void replyError(ZBytes error) {
        requireNotReplied();
        replied = true;
        session.sendReplyError(requestId, keyExpr, error);
    }

    /** Returns the request ID used to correlate this query with its replies. */
    long requestId() {
        return requestId;
    }

    private void requireNotReplied() {
        if (replied) throw new IllegalStateException("Query has already been replied to");
    }
}
