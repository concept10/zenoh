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
import io.zenoh.internal.ZenohTransport;
import io.zenoh.internal.codec.ZenohEncoder;
import io.zenoh.internal.messages.Messages;
import io.zenoh.internal.messages.Messages.*;

/**
 * A Zenoh publisher for a specific key expression.
 *
 * <p>A publisher is declared via {@link Session#declarePublisher(KeyExpr)} and
 * allows sending data with {@link #put(ZBytes)} or {@link #delete()}.
 *
 * <pre>{@code
 * Publisher pub = session.declarePublisher(KeyExpr.of("demo/example")).await();
 * pub.put(ZBytes.of("Hello!"));
 * pub.close();
 * }</pre>
 */
public final class Publisher implements AutoCloseable {

    private final KeyExpr keyExpr;
    private final ZenohTransport transport;
    private final Session session;
    private volatile boolean closed = false;

    private Encoding encoding = Encoding.ZENOH_BYTES;
    private Priority priority = Priority.DATA;
    private CongestionControl congestionControl = CongestionControl.DROP;

    Publisher(KeyExpr keyExpr, ZenohTransport transport, Session session) {
        this.keyExpr   = keyExpr;
        this.transport = transport;
        this.session   = session;
    }

    // -----------------------------------------------------------------------
    // Configuration (builder-style fluent setters)
    // -----------------------------------------------------------------------

    /** Sets the default encoding for subsequent {@link #put} calls. */
    public Publisher encoding(Encoding encoding) {
        this.encoding = encoding;
        return this;
    }

    /** Sets the QoS priority for messages sent by this publisher. */
    public Publisher priority(Priority priority) {
        this.priority = priority;
        return this;
    }

    /** Sets the congestion-control behaviour. */
    public Publisher congestionControl(CongestionControl cc) {
        this.congestionControl = cc;
        return this;
    }

    // -----------------------------------------------------------------------
    // Publishing
    // -----------------------------------------------------------------------

    /**
     * Publishes a PUT sample on {@link #keyExpr()}.
     *
     * @param payload the payload bytes
     */
    public void put(ZBytes payload) {
        put(payload, encoding);
    }

    /**
     * Publishes a PUT sample with the given payload and encoding.
     */
    public void put(ZBytes payload, Encoding enc) {
        requireOpen();
        WireExpr wireExpr = keyExprToWireExpr(keyExpr);
        Messages.Put put  = new Messages.Put(payload, enc);
        Push push         = new Push(wireExpr, put);
        long sn           = transport.nextReliableSn();
        ByteBuf buf       = ZenohEncoder.encodeFrameWithPush(sn, push, true);
        transport.send(buf);
    }

    /**
     * Publishes a PUT sample from a UTF-8 string.
     */
    public void put(String text) {
        put(ZBytes.of(text), Encoding.TEXT_PLAIN);
    }

    /**
     * Publishes a DELETE sample on {@link #keyExpr()}.
     */
    public void delete() {
        requireOpen();
        WireExpr wireExpr = keyExprToWireExpr(keyExpr);
        Push push         = new Push(wireExpr, new Del());
        long sn           = transport.nextReliableSn();
        ByteBuf buf       = ZenohEncoder.encodeFrameWithPush(sn, push, true);
        transport.send(buf);
    }

    /** Returns the key expression this publisher is bound to. */
    public KeyExpr keyExpr() {
        return keyExpr;
    }

    /** Returns {@code true} if this publisher has been closed. */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            session.removePublisher(this);
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void requireOpen() {
        if (closed) throw new IllegalStateException("Publisher is closed");
    }

    static WireExpr keyExprToWireExpr(KeyExpr keyExpr) {
        return new WireExpr(0, keyExpr.asStr(), true);
    }
}
