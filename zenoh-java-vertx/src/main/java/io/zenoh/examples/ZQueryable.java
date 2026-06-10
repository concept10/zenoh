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
 * Example: declare a queryable and reply to every incoming query.
 *
 * <pre>
 * Usage: ZQueryable [key]
 *   key – key expression to serve (default: demo/example/vertx-queryable)
 * </pre>
 */
public class ZQueryable {

    public static void main(String[] args) throws Exception {
        String keyStr = args.length > 0 ? args[0] : "demo/example/vertx-queryable";

        Vertx vertx = Vertx.vertx();
        CountDownLatch done = new CountDownLatch(1);

        Zenoh.open(Config.defaultConfig(), vertx).onSuccess(session -> {
            session.declareQueryable(KeyExpr.of(keyStr), query -> {
                System.out.printf("[ZQueryable] >> Received query on '%s'%n", query.keyExpr());
                query.reply(query.keyExpr(), ZBytes.of("Queryable reply from Vert.x!"));
            }).onSuccess(qbl -> {
                System.out.printf("[ZQueryable] Queryable registered on '%s'. Press CTRL-C to quit.%n", keyStr);
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    qbl.close();
                    session.close();
                    vertx.close().onComplete(v -> done.countDown());
                }));
            });
        }).onFailure(err -> {
            System.err.println("[ZQueryable] Failed to open session: " + err.getMessage());
            vertx.close().onComplete(v -> done.countDown());
        });

        done.await();
    }
}
