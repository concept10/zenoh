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

import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests that require a running Zenoh router on localhost:7447.
 *
 * <p>These tests are skipped unless the system property
 * {@code zenoh.test.router} is set to {@code "true"}.
 *
 * <pre>
 * mvn test -Dzenoh.test.router=true
 * </pre>
 */
@EnabledIfSystemProperty(named = "zenoh.test.router", matches = "true")
class SessionTest {

    private Vertx vertx;
    private Session session;

    @BeforeEach
    void setUp() throws Exception {
        vertx = Vertx.vertx();
        CompletableFuture<Session> cf = Zenoh.open(Config.defaultConfig(), vertx)
                .toCompletionStage()
                .toCompletableFuture();
        session = cf.get(10, TimeUnit.SECONDS);
        assertNotNull(session);
        assertTrue(session.isOpen());
    }

    @AfterEach
    void tearDown() {
        if (session != null) session.close();
        if (vertx != null)   vertx.close();
    }

    // -----------------------------------------------------------------------
    // Basic put / subscribe round-trip
    // -----------------------------------------------------------------------

    @Test
    void pubSubRoundTrip() throws Exception {
        String keyStr = "test/session/pubsub";
        CompletableFuture<Sample> received = new CompletableFuture<>();

        Subscriber sub = session.declareSubscriber(KeyExpr.of(keyStr), received::complete)
                .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertNotNull(sub);

        // give the router time to set up routing
        Thread.sleep(300);

        session.put(KeyExpr.of(keyStr), "Hello from SessionTest");

        Sample sample = received.get(5, TimeUnit.SECONDS);
        assertNotNull(sample);
        assertEquals(keyStr, sample.keyExpr().toString());
        assertEquals("Hello from SessionTest", sample.payload().toString());

        sub.close();
    }

    // -----------------------------------------------------------------------
    // Queryable / get round-trip
    // -----------------------------------------------------------------------

    @Test
    void queryableGetRoundTrip() throws Exception {
        String keyStr = "test/session/queryable";
        String answer = "QueryAnswer";

        Queryable qbl = session.declareQueryable(KeyExpr.of(keyStr), query ->
                query.reply(query.keyExpr(), ZBytes.of(answer))
        ).toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertNotNull(qbl);

        Thread.sleep(300);

        List<Reply> replies = new ArrayList<>();
        CompletableFuture<Void> done = session
                .get(KeyExpr.of(keyStr), replies::add, 3000)
                .toCompletionStage().toCompletableFuture();
        done.get(5, TimeUnit.SECONDS);

        assertFalse(replies.isEmpty(), "Should have received at least one reply");
        Reply r = replies.get(0);
        assertTrue(r.isOk());
        assertEquals(answer, r.getSample().payload().toString());

        qbl.close();
    }

    // -----------------------------------------------------------------------
    // Delete produces a DELETE sample
    // -----------------------------------------------------------------------

    @Test
    void deleteProducesDeleteSample() throws Exception {
        String keyStr = "test/session/delete";
        CompletableFuture<Sample> received = new CompletableFuture<>();

        Subscriber sub = session.declareSubscriber(KeyExpr.of(keyStr), received::complete)
                .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);

        Thread.sleep(300);
        session.delete(KeyExpr.of(keyStr));

        Sample sample = received.get(5, TimeUnit.SECONDS);
        assertEquals(SampleKind.DELETE, sample.kind());

        sub.close();
    }

    // -----------------------------------------------------------------------
    // Session closes cleanly
    // -----------------------------------------------------------------------

    @Test
    void sessionCloseIsIdempotent() {
        assertTrue(session.isOpen());
        session.close();
        assertFalse(session.isOpen());
        // second close should not throw
        assertDoesNotThrow(() -> session.close());
        session = null; // prevent tearDown from closing again
    }
}
