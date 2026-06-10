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
package io.zenoh.examples;

import io.vertx.core.Vertx;
import io.zenoh.*;

import java.util.concurrent.CountDownLatch;

/**
 * Example: perform a GET query and print all received replies.
 *
 * <pre>
 * Usage: ZGet [selector] [timeoutMs]
 *   selector  – key expression selector (default: demo/example/**)
 *   timeoutMs – how long to wait in ms  (default: 5000)
 * </pre>
 */
public class ZGet {

    public static void main(String[] args) throws Exception {
        String selector   = args.length > 0 ? args[0] : "demo/example/**";
        long   timeoutMs  = args.length > 1 ? Long.parseLong(args[1]) : 5000L;

        Vertx vertx = Vertx.vertx();
        CountDownLatch done = new CountDownLatch(1);

        Zenoh.open(Config.defaultConfig(), vertx).onSuccess(session -> {
            System.out.printf("[ZGet] Sending query '%s' (timeout=%dms)%n", selector, timeoutMs);

            session.get(KeyExpr.of(selector), reply -> {
                if (reply.isOk()) {
                    Sample s = reply.getSample();
                    System.out.printf("[ZGet] >> Received ('%s': '%s')%n",
                            s.keyExpr(), s.payload().toString());
                } else {
                    System.out.printf("[ZGet] >> Received error: '%s'%n",
                            reply.getError().toString());
                }
            }, timeoutMs).onComplete(v -> {
                System.out.println("[ZGet] Done.");
                session.close();
                vertx.close().onComplete(x -> done.countDown());
            });

        }).onFailure(err -> {
            System.err.println("[ZGet] Failed to open session: " + err.getMessage());
            vertx.close().onComplete(v -> done.countDown());
        });

        done.await();
    }
}
