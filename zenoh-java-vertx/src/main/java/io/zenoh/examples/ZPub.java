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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Example: declare a publisher and periodically publish a counter value.
 *
 * <pre>
 * Usage: ZPub [key] [count]
 *   key   – key expression to publish on (default: demo/example/vertx-zpub)
 *   count – number of messages to send   (default: 100)
 * </pre>
 */
public class ZPub {

    public static void main(String[] args) throws Exception {
        String keyStr = args.length > 0 ? args[0] : "demo/example/vertx-zpub";
        int    count  = args.length > 1 ? Integer.parseInt(args[1]) : 100;

        Vertx vertx = Vertx.vertx();
        CountDownLatch done = new CountDownLatch(1);
        AtomicLong counter  = new AtomicLong(0);

        Zenoh.open(Config.defaultConfig(), vertx).onSuccess(session -> {
            Publisher pub = session.declarePublisher(KeyExpr.of(keyStr));
            System.out.printf("[ZPub] Publishing %d messages on '%s'%n", count, keyStr);

            long timerId = vertx.setPeriodic(1000, id -> {
                long n = counter.incrementAndGet();
                String msg = "Pub from Vert.x [" + n + "]";
                System.out.println("[ZPub] Sending: " + msg);
                pub.put(ZBytes.of(msg));

                if (n >= count) {
                    vertx.cancelTimer(id);
                    pub.close();
                    session.close();
                    vertx.close().onComplete(v -> done.countDown());
                }
            });
        }).onFailure(err -> {
            System.err.println("[ZPub] Failed to open session: " + err.getMessage());
            vertx.close().onComplete(v -> done.countDown());
        });

        done.await();
    }
}
