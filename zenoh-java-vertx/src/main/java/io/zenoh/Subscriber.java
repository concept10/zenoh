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
 * A Zenoh subscriber that receives {@link Sample}s for a specific key expression.
 *
 * <p>Subscribers are declared via {@link Session#declareSubscriber(KeyExpr, Consumer)}
 * and automatically inform the router of interest via a {@code DeclareSubscriber} message.
 *
 * <pre>{@code
 * Subscriber sub = session.declareSubscriber(
 *     KeyExpr.of("demo/example/**"),
 *     sample -> System.out.println("Received: " + sample)
 * ).await();
 * // ... later
 * sub.close();
 * }</pre>
 */
public final class Subscriber implements AutoCloseable {

    private final long id;
    private final KeyExpr keyExpr;
    private final Consumer<Sample> callback;
    private final Session session;
    private volatile boolean closed = false;

    Subscriber(long id, KeyExpr keyExpr, Consumer<Sample> callback, Session session) {
        this.id       = id;
        this.keyExpr  = keyExpr;
        this.callback = callback;
        this.session  = session;
    }

    /** Returns the unique subscriber ID assigned by this session. */
    public long id() {
        return id;
    }

    /** Returns the key expression this subscriber is bound to. */
    public KeyExpr keyExpr() {
        return keyExpr;
    }

    /** Delivers a sample to the registered callback. */
    void deliver(Sample sample) {
        if (!closed) {
            try {
                callback.accept(sample);
            } catch (Exception e) {
                // Subscriber callbacks must not throw; log and continue
                org.slf4j.LoggerFactory.getLogger(Subscriber.class)
                        .error("Subscriber callback threw", e);
            }
        }
    }

    /** Returns {@code true} if this subscriber has been closed. */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            session.removeSubscriber(this);
        }
    }
}
