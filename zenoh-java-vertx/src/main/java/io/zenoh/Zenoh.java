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
import io.vertx.core.Vertx;
import io.zenoh.internal.ZenohTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;

/**
 * Main entry point for the Zenoh Java / Vert.x implementation.
 *
 * <pre>{@code
 * // Open with default config (connects to tcp/localhost:7447)
 * Vertx vertx = Vertx.vertx();
 * Session session = Zenoh.open(Config.defaultConfig(), vertx).toCompletionStage().toCompletableFuture().get();
 *
 * session.put(KeyExpr.of("demo/hello"), ZBytes.of("World"));
 * session.close().toCompletionStage().toCompletableFuture().get();
 * vertx.close();
 * }</pre>
 *
 * <p>If you are already inside a Vert.x context (i.e., from a verticle or a
 * handler), use {@link #open(Config, Vertx)} directly to avoid creating a
 * second Vert.x instance.
 */
public final class Zenoh {

    private static final Logger log = LoggerFactory.getLogger(Zenoh.class);

    private Zenoh() {}

    // -----------------------------------------------------------------------
    // Factory methods
    // -----------------------------------------------------------------------

    /**
     * Opens a new {@link Session} using the provided {@link Config} and the
     * given {@link Vertx} instance.
     *
     * @param config the session configuration
     * @param vertx  the Vert.x instance to use for async I/O
     * @return a {@link Future} that completes with the connected session
     */
    public static Future<Session> open(Config config, Vertx vertx) {
        // Determine connection endpoint
        String host = "localhost";
        int    port = 7447;

        List<String> endpoints = config.connectEndpoints();
        if (endpoints != null && !endpoints.isEmpty()) {
            String ep = endpoints.get(0); // e.g. "tcp/127.0.0.1:7447"
            try {
                // Strip the leading scheme (e.g. "tcp/")
                String address = ep;
                int slashIdx = ep.indexOf('/');
                if (slashIdx >= 0) {
                    address = ep.substring(slashIdx + 1);
                }
                // address is now "host:port"
                URI uri = new URI("tcp://" + address);
                host = uri.getHost();
                port = uri.getPort() > 0 ? uri.getPort() : 7447;
            } catch (Exception e) {
                log.warn("Could not parse endpoint '{}', using default {}:{}: {}",
                        ep, host, port, e.getMessage());
            }
        }

        ZenohTransport transport = new ZenohTransport(vertx, host, port);

        return transport.connect().map(ignored -> new Session(transport, vertx));
    }

    /**
     * Opens a new {@link Session} using default configuration and a freshly
     * created Vert.x instance owned by the caller.
     *
     * <p><b>Note:</b> This method creates a new {@link Vertx} instance. The
     * caller is responsible for closing it when done.
     *
     * @param config the session configuration
     * @return a {@link Future} that completes with the connected session
     */
    public static Future<Session> open(Config config) {
        return open(config, Vertx.vertx());
    }
}
