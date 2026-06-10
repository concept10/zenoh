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

import java.util.function.Consumer;

/**
 * A Zenoh queryable that responds to get queries matching a key expression.
 *
 * <p>Declare a queryable via {@link Session#declareQueryable(KeyExpr, Consumer)}.
 * Incoming {@link Query} objects are delivered to the handler callback.
 *
 * <pre>{@code
 * Queryable qbl = session.declareQueryable(
 *     KeyExpr.of("demo/example/queryable"),
 *     query -> query.reply(query.keyExpr(), ZBytes.of("Response!"))
 * ).await();
 * // ... later
 * qbl.close();
 * }</pre>
 */
public final class Queryable implements AutoCloseable {

    private final long id;
    private final KeyExpr keyExpr;
    private final Consumer<Query> handler;
    private final Session session;
    private volatile boolean closed = false;

    Queryable(long id, KeyExpr keyExpr, Consumer<Query> handler, Session session) {
        this.id      = id;
        this.keyExpr = keyExpr;
        this.handler = handler;
        this.session = session;
    }

    /** Returns the unique queryable ID assigned by this session. */
    public long id() {
        return id;
    }

    /** Returns the key expression this queryable is bound to. */
    public KeyExpr keyExpr() {
        return keyExpr;
    }

    /** Delivers an incoming query to the registered handler. */
    void deliver(Query query) {
        if (!closed) {
            try {
                handler.accept(query);
            } catch (Exception e) {
                org.slf4j.LoggerFactory.getLogger(Queryable.class)
                        .error("Queryable handler threw", e);
            }
        }
    }

    /** Returns {@code true} if this queryable has been closed. */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            session.removeQueryable(this);
        }
    }
}
