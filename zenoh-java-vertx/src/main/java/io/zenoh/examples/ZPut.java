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
 * Example: publish a single PUT value and exit.
 *
 * <pre>
 * Usage: ZPut [key] [value]
 *   key   – key expression       (default: demo/example/vertx-put)
 *   value – string value to put  (default: "Put from Vert.x!")
 * </pre>
 */
public class ZPut {

    public static void main(String[] args) throws Exception {
        String keyStr   = args.length > 0 ? args[0] : "demo/example/vertx-put";
        String valueStr = args.length > 1 ? args[1] : "Put from Vert.x!";

        Vertx vertx = Vertx.vertx();
        CountDownLatch done = new CountDownLatch(1);

        Zenoh.open(Config.defaultConfig(), vertx).onSuccess(session -> {
            System.out.printf("[ZPut] Putting '%s' on '%s'%n", valueStr, keyStr);
            session.put(KeyExpr.of(keyStr), valueStr);
            session.close();
            vertx.close().onComplete(v -> done.countDown());
        }).onFailure(err -> {
            System.err.println("[ZPut] Failed to open session: " + err.getMessage());
            vertx.close().onComplete(v -> done.countDown());
        });

        done.await();
    }
}
