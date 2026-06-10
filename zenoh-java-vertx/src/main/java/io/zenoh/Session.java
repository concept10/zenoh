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
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.zenoh.internal.ZenohTransport;
import io.zenoh.internal.codec.ZenohEncoder;
import io.zenoh.internal.messages.Messages;
import io.zenoh.internal.messages.Messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * A Zenoh session.
 *
 * <p>A session is the main entry point for all Zenoh operations.
 * Use {@link Zenoh#open(Config, Vertx)} to create a session.
 *
 * <pre>{@code
 * Vertx vertx = Vertx.vertx();
 * Session session = Zenoh.open(Config.defaultConfig(), vertx).await();
 *
 * // Publish
 * session.put(KeyExpr.of("demo/hello"), ZBytes.of("World")).await();
 *
 * // Subscribe
 * Subscriber sub = session.declareSubscriber(
 *     KeyExpr.of("demo/**"),
 *     sample -> System.out.println(sample)
 * ).await();
 *
 * // Queryable
 * Queryable qbl = session.declareQueryable(
 *     KeyExpr.of("demo/query"),
 *     query -> query.reply(query.keyExpr(), ZBytes.of("answer"))
 * ).await();
 *
 * // Get
 * session.get(KeyExpr.of("demo/**"), reply -> System.out.println(reply), 5000).await();
 *
 * session.close().await();
 * }</pre>
 */
public final class Session implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Session.class);

    private final ZenohTransport transport;
    private final Vertx vertx;
    private volatile boolean closed = false;

    // ID generators
    private final AtomicLong subscriberIdGen  = new AtomicLong(1);
    private final AtomicLong queryableIdGen   = new AtomicLong(1);
    private final AtomicLong interestIdGen    = new AtomicLong(1);
    private final AtomicLong requestIdGen     = new AtomicLong(1);

    // Registered subscribers keyed by subscriber ID
    private final Map<Long, Subscriber> subscribers = new ConcurrentHashMap<>();

    // Registered queryables keyed by queryable ID
    private final Map<Long, Queryable> queryables = new ConcurrentHashMap<>();

    // Publishers (tracked for cleanup)
    private final List<Publisher> publishers = new CopyOnWriteArrayList<>();

    // Pending get callbacks keyed by request ID
    private final Map<Long, GetContext> pendingGets = new ConcurrentHashMap<>();

    // Resource table: scope ID → key expression string
    private final Map<Long, String> resourceTable = new ConcurrentHashMap<>();

    // -----------------------------------------------------------------------

    Session(ZenohTransport transport, Vertx vertx) {
        this.transport = transport;
        this.vertx     = vertx;
        transport.messageHandler(this::handleNetworkMessage);
        transport.closeHandler(() -> {
            closed = true;
            log.info("Transport closed");
        });
    }

    // -----------------------------------------------------------------------
    // Put / Delete (convenience methods without a Publisher)
    // -----------------------------------------------------------------------

    /**
     * Publishes a single PUT sample.
     *
     * @param keyExpr the target key expression
     * @param payload the payload bytes
     */
    public void put(KeyExpr keyExpr, ZBytes payload) {
        put(keyExpr, payload, Encoding.ZENOH_BYTES);
    }

    /**
     * Publishes a single PUT sample with explicit encoding.
     */
    public void put(KeyExpr keyExpr, ZBytes payload, Encoding encoding) {
        requireOpen();
        WireExpr wireExpr   = Publisher.keyExprToWireExpr(keyExpr);
        Messages.Put put    = new Messages.Put(payload, encoding);
        Push push           = new Push(wireExpr, put);
        long sn             = transport.nextReliableSn();
        ByteBuf buf         = ZenohEncoder.encodeFrameWithPush(sn, push, true);
        transport.send(buf);
    }

    /**
     * Publishes a single PUT sample from a UTF-8 string.
     */
    public void put(KeyExpr keyExpr, String payload) {
        put(keyExpr, ZBytes.of(payload), Encoding.TEXT_PLAIN);
    }

    /**
     * Publishes a single DELETE sample.
     */
    public void delete(KeyExpr keyExpr) {
        requireOpen();
        WireExpr wireExpr = Publisher.keyExprToWireExpr(keyExpr);
        Push push         = new Push(wireExpr, new Del());
        long sn           = transport.nextReliableSn();
        ByteBuf buf       = ZenohEncoder.encodeFrameWithPush(sn, push, true);
        transport.send(buf);
    }

    // -----------------------------------------------------------------------
    // Publisher
    // -----------------------------------------------------------------------

    /**
     * Declares a {@link Publisher} for the given key expression.
     * The publisher lifetime is tied to explicit {@link Publisher#close()} calls.
     *
     * @param keyExpr the key expression to publish on
     * @return the newly declared publisher
     */
    public Publisher declarePublisher(KeyExpr keyExpr) {
        requireOpen();
        Publisher pub = new Publisher(keyExpr, transport, this);
        publishers.add(pub);
        return pub;
    }

    // -----------------------------------------------------------------------
    // Subscriber
    // -----------------------------------------------------------------------

    /**
     * Declares a {@link Subscriber} and sends a {@code DeclareSubscriber} message
     * to the router so it routes matching publications to this session.
     *
     * @param keyExpr  the key expression to subscribe to (may use wildcards)
     * @param callback the handler invoked for each received {@link Sample}
     * @return a {@link Future} that completes with the new {@link Subscriber}
     */
    public Future<Subscriber> declareSubscriber(KeyExpr keyExpr, Consumer<Sample> callback) {
        requireOpen();
        long subId = subscriberIdGen.getAndIncrement();
        Subscriber sub = new Subscriber(subId, keyExpr, callback, this);
        subscribers.put(subId, sub);

        // Send Interest(CurrentFuture, subscribers) then DeclareSubscriber
        long interestId = interestIdGen.getAndIncrement();
        WireExpr wireExpr = Publisher.keyExprToWireExpr(keyExpr);

        Interest interest = new Interest(
                interestId,
                Interest.MODE_CURRENT_FUTURE,
                Interest.OPT_SUBSCRIBERS | Interest.OPT_KEY_EXPRS,
                wireExpr
        );
        long snInterest = transport.nextReliableSn();
        transport.send(ZenohEncoder.encodeFrameWithInterest(snInterest, interest));

        // Declare the subscriber
        DeclareSubscriber decl = new DeclareSubscriber(subId, wireExpr);
        long sn = transport.nextReliableSn();
        transport.send(ZenohEncoder.encodeFrameWithDeclareSubscriber(sn, decl));

        return Future.succeededFuture(sub);
    }

    // -----------------------------------------------------------------------
    // Queryable
    // -----------------------------------------------------------------------

    /**
     * Declares a {@link Queryable} and sends a {@code DeclareQueryable} message
     * to the router.
     *
     * @param keyExpr the key expression this queryable serves
     * @param handler the handler invoked for each incoming {@link Query}
     * @return a {@link Future} that completes with the new {@link Queryable}
     */
    public Future<Queryable> declareQueryable(KeyExpr keyExpr, Consumer<Query> handler) {
        requireOpen();
        long qId = queryableIdGen.getAndIncrement();
        Queryable qbl = new Queryable(qId, keyExpr, handler, this);
        queryables.put(qId, qbl);

        WireExpr wireExpr = Publisher.keyExprToWireExpr(keyExpr);
        DeclareQueryable decl = new DeclareQueryable(qId, wireExpr, false);
        long sn = transport.nextReliableSn();
        transport.send(ZenohEncoder.encodeFrameWithDeclareQueryable(sn, decl));

        return Future.succeededFuture(qbl);
    }

    // -----------------------------------------------------------------------
    // Get (query)
    // -----------------------------------------------------------------------

    /**
     * Sends a query and collects all replies within a timeout.
     *
     * @param keyExpr       the selector key expression
     * @param replyHandler  called once for each reply received
     * @param timeoutMs     how long to wait for replies (milliseconds)
     * @return a {@link Future} that completes when all replies have been received
     *         or the timeout elapses
     */
    public Future<Void> get(KeyExpr keyExpr, Consumer<Reply> replyHandler, long timeoutMs) {
        requireOpen();
        long requestId = requestIdGen.getAndIncrement();
        Promise<Void> promise = Promise.promise();

        GetContext ctx = new GetContext(replyHandler, promise);
        pendingGets.put(requestId, ctx);

        WireExpr wireExpr = Publisher.keyExprToWireExpr(keyExpr);
        NetRequest request = new NetRequest(requestId, wireExpr, ZBytes.empty(), timeoutMs);
        long sn = transport.nextReliableSn();
        transport.send(ZenohEncoder.encodeFrameWithRequest(sn, request));

        // Set a timer to complete the future if no ResponseFinal arrives in time
        vertx.setTimer(timeoutMs + 500, id -> {
            GetContext removed = pendingGets.remove(requestId);
            if (removed != null && !promise.future().isComplete()) {
                promise.complete();
            }
        });

        return promise.future();
    }

    // -----------------------------------------------------------------------
    // Session info / close
    // -----------------------------------------------------------------------

    /** Returns {@code true} if this session is open. */
    public boolean isOpen() {
        return !closed;
    }

    /** Closes this session and releases all resources. */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            // Undeclare all subscribers
            for (Subscriber sub : subscribers.values()) {
                sendUndeclareSubscriber(sub.id());
            }
            subscribers.clear();
            queryables.clear();
            publishers.clear();
            transport.close();
        }
    }

    // -----------------------------------------------------------------------
    // Package-private callbacks used by Publisher / Subscriber / Queryable
    // -----------------------------------------------------------------------

    void removePublisher(Publisher pub) {
        publishers.remove(pub);
    }

    void removeSubscriber(Subscriber sub) {
        if (subscribers.remove(sub.id()) != null) {
            sendUndeclareSubscriber(sub.id());
        }
    }

    void removeQueryable(Queryable qbl) {
        if (queryables.remove(qbl.id()) != null) {
            UndeclareQueryable undecl = new UndeclareQueryable(qbl.id());
            long sn = transport.nextReliableSn();
            transport.send(ZenohEncoder.encodeFrameWithNetMsg(sn, undecl));
        }
    }

    /**
     * Sends a reply to an incoming {@link Query}.
     * Called by {@link Query#reply}.
     */
    void sendReply(long requestId, KeyExpr keyExpr, ZBytes payload, Encoding encoding) {
        requireOpen();
        // Encode a RESPONSE message
        WireExpr wireExpr = Publisher.keyExprToWireExpr(keyExpr);
        Messages.Put put  = new Messages.Put(payload, encoding);
        NetResponse resp  = new NetResponse(requestId, wireExpr, put);
        long sn           = transport.nextReliableSn();
        transport.send(ZenohEncoder.encodeFrameWithResponse(sn, resp));

        // Send ResponseFinal
        ResponseFinal final_ = new ResponseFinal(requestId);
        long sn2 = transport.nextReliableSn();
        transport.send(ZenohEncoder.encodeFrameWithResponseFinal(sn2, final_));
    }

    /**
     * Sends an error reply to an incoming {@link Query}.
     * Called by {@link Query#replyError}.
     */
    void sendReplyError(long requestId, KeyExpr keyExpr, ZBytes error) {
        requireOpen();
        WireExpr wireExpr   = Publisher.keyExprToWireExpr(keyExpr);
        NetResponse resp    = new NetResponse(requestId, wireExpr, error);
        long sn             = transport.nextReliableSn();
        transport.send(ZenohEncoder.encodeFrameWithResponse(sn, resp));

        ResponseFinal final_ = new ResponseFinal(requestId);
        long sn2 = transport.nextReliableSn();
        transport.send(ZenohEncoder.encodeFrameWithResponseFinal(sn2, final_));
    }

    // -----------------------------------------------------------------------
    // Incoming message dispatch
    // -----------------------------------------------------------------------

    private void handleNetworkMessage(Object msg) {
        if (msg instanceof Push push) {
            dispatchPush(push);
        } else if (msg instanceof DeclareKeyExpr dke) {
            // Router declares a resource mapping: scope → key expression string
            resourceTable.put(dke.exprId(), resolveWireExpr(dke.wireExpr()));
        } else if (msg instanceof NetResponse resp) {
            dispatchResponse(resp);
        } else if (msg instanceof ResponseFinal rf) {
            GetContext ctx = pendingGets.remove(rf.requestId());
            if (ctx != null) {
                ctx.promise().complete();
            }
        } else if (msg instanceof Messages.NetRequest req) {
            dispatchRequest(req);
        }
    }

    private void dispatchPush(Push push) {
        String keyStr = resolveWireExpr(push.wireExpr);
        if (keyStr == null || keyStr.isEmpty()) return;
        KeyExpr keyExpr = KeyExpr.of(keyStr);

        ZBytes payload;
        Encoding encoding;
        SampleKind kind;

        if (push.isPut()) {
            payload  = push.put.payload() != null ? push.put.payload() : ZBytes.empty();
            encoding = push.put.encoding() != null ? push.put.encoding() : Encoding.ZENOH_BYTES;
            kind     = SampleKind.PUT;
        } else {
            payload  = ZBytes.empty();
            encoding = Encoding.ZENOH_BYTES;
            kind     = SampleKind.DELETE;
        }

        Sample sample = new Sample(keyExpr, payload, encoding, kind);

        for (Subscriber sub : subscribers.values()) {
            if (keyExpr.intersects(sub.keyExpr())) {
                sub.deliver(sample);
            }
        }
    }

    private void dispatchResponse(NetResponse resp) {
        GetContext ctx = pendingGets.get(resp.requestId);
        if (ctx == null) return;

        String keyStr = resolveWireExpr(resp.wireExpr);
        KeyExpr keyExpr = keyStr != null && !keyStr.isEmpty()
                ? KeyExpr.of(keyStr)
                : KeyExpr.of("unknown");

        Reply reply;
        if (resp.isOk()) {
            ZBytes payload  = resp.put.payload() != null ? resp.put.payload() : ZBytes.empty();
            Encoding enc    = resp.put.encoding() != null ? resp.put.encoding() : Encoding.ZENOH_BYTES;
            Sample sample   = new Sample(keyExpr, payload, enc, SampleKind.PUT);
            reply           = Reply.ok(sample);
        } else {
            reply = Reply.error(resp.error != null ? resp.error : ZBytes.empty());
        }

        ctx.handler().accept(reply);
    }

    private void dispatchRequest(NetRequest req) {
        String keyStr = resolveWireExpr(req.wireExpr);
        if (keyStr == null || keyStr.isEmpty()) return;
        KeyExpr keyExpr = KeyExpr.of(keyStr);

        for (Queryable qbl : queryables.values()) {
            if (keyExpr.intersects(qbl.keyExpr())) {
                ZBytes payload  = req.payload != null ? req.payload : ZBytes.empty();
                Query query     = new Query(req.requestId, keyExpr, payload,
                        Encoding.ZENOH_BYTES, this);
                qbl.deliver(query);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    private String resolveWireExpr(WireExpr wireExpr) {
        if (wireExpr.suffix() != null && !wireExpr.suffix().isEmpty()) {
            if (wireExpr.scope() != 0) {
                // Prefix from resource table + suffix
                String prefix = resourceTable.get(wireExpr.scope());
                return prefix != null ? prefix + wireExpr.suffix() : wireExpr.suffix();
            }
            return wireExpr.suffix();
        }
        // No suffix: scope is the full resource ID
        if (wireExpr.scope() != 0) {
            return resourceTable.getOrDefault(wireExpr.scope(), String.valueOf(wireExpr.scope()));
        }
        return "";
    }

    private void sendUndeclareSubscriber(long subId) {
        UndeclareSubscriber undecl = new UndeclareSubscriber(subId);
        long sn = transport.nextReliableSn();
        try {
            transport.send(ZenohEncoder.encodeFrameWithUndeclareSubscriber(sn, undecl));
        } catch (Exception e) {
            log.debug("Could not send UndeclareSubscriber: {}", e.getMessage());
        }
    }

    private void requireOpen() {
        if (closed) throw new IllegalStateException("Session is closed");
    }

    // -----------------------------------------------------------------------
    // Helper record for pending get contexts
    // -----------------------------------------------------------------------

    private record GetContext(Consumer<Reply> handler, Promise<Void> promise) {}
}
