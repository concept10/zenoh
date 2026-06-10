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
package io.zenoh.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;
import io.zenoh.internal.codec.VarInt;
import io.zenoh.internal.codec.ZenohDecoder;
import io.zenoh.internal.codec.ZenohEncoder;
import io.zenoh.internal.messages.Messages;
import io.zenoh.internal.messages.Messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Manages the Zenoh transport session over a TCP connection using Vert.x.
 *
 * <p>Responsibilities:
 * <ol>
 *   <li>TCP connection via {@link NetClient}.</li>
 *   <li>Zenoh INIT/OPEN handshake.</li>
 *   <li>Framing: prepend / strip the 2-byte little-endian length prefix.</li>
 *   <li>Dispatching decoded {@link ZenohDecoder.DecodedFrame} network messages
 *       to the registered {@link #messageHandler}.</li>
 *   <li>Providing {@link #send(ByteBuf)} to write encoded messages.</li>
 * </ol>
 */
public final class ZenohTransport {

    private static final Logger log = LoggerFactory.getLogger(ZenohTransport.class);

    private final Vertx vertx;
    private final String defaultHost;
    private final int defaultPort;
    private final AtomicLong snReliable   = new AtomicLong(0);
    private final AtomicBoolean connected = new AtomicBoolean(false);

    private NetSocket socket;
    /** A partial-batch accumulation buffer (handles TCP fragmentation). */
    private ByteBuf accumulator = Unpooled.buffer(4096);

    /** Called for every decoded network-layer message received from the router. */
    private Consumer<Object> messageHandler = msg -> {};

    /** Called when the transport is closed (normally or due to error). */
    private Runnable closeHandler = () -> {};

    public ZenohTransport(Vertx vertx) {
        this(vertx, "localhost", 7447);
    }

    public ZenohTransport(Vertx vertx, String host, int port) {
        this.vertx       = vertx;
        this.defaultHost = host;
        this.defaultPort = port;
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Sets the handler that receives decoded network-layer message objects.
     * Must be called before {@link #connect}.
     */
    public ZenohTransport messageHandler(Consumer<Object> handler) {
        this.messageHandler = handler;
        return this;
    }

    /** Sets the handler called when the transport is closed. */
    public ZenohTransport closeHandler(Runnable handler) {
        this.closeHandler = handler;
        return this;
    }

    /**
     * Connects to the default host/port and performs the Zenoh INIT/OPEN handshake.
     *
     * @return a {@link Future} that completes when the session is open and ready.
     */
    public Future<Void> connect() {
        return connect(defaultHost, defaultPort);
    }

    /**
     * Connects to {@code host:port} and performs the Zenoh INIT/OPEN handshake.
     *
     * @return a {@link Future} that completes when the session is open and ready.
     */
    public Future<Void> connect(String host, int port) {
        Promise<Void> handshakePromise = Promise.promise();

        NetClientOptions opts = new NetClientOptions()
                .setConnectTimeout(5000)
                .setReconnectAttempts(0);

        NetClient client = vertx.createNetClient(opts);
        client.connect(port, host).onComplete(ar -> {
            if (ar.failed()) {
                handshakePromise.fail(ar.cause());
                return;
            }
            socket = ar.result();
            socket.closeHandler(v -> {
                connected.set(false);
                closeHandler.run();
            });
            socket.exceptionHandler(err -> log.warn("Socket error: {}", err.getMessage()));
            socket.handler(buf -> handleData(buf, handshakePromise));

            // Start INIT/OPEN handshake
            byte[] zid = generateZid();
            InitSyn initSyn = new InitSyn(Messages.ZENOH_VERSION, Messages.WHATAMI_CLIENT, zid);
            ByteBuf encoded = ZenohEncoder.encodeInitSyn(initSyn);
            sendRaw(encoded);
        });

        return handshakePromise.future();
    }

    /**
     * Sends an already-framed (length-prefixed) message to the router.
     *
     * @param buf a framed {@link ByteBuf}; ownership is transferred — the caller
     *            must not use it after this call.
     */
    public void send(ByteBuf buf) {
        if (!connected.get()) {
            buf.release();
            throw new IllegalStateException("Not connected");
        }
        sendRaw(buf);
    }

    /** Returns the next reliable sequence number (monotonically increasing). */
    public long nextReliableSn() {
        return snReliable.getAndIncrement();
    }

    /** Returns {@code true} if the transport handshake has completed. */
    public boolean isConnected() {
        return connected.get();
    }

    /** Closes the TCP connection and releases resources. */
    public Future<Void> close() {
        connected.set(false);
        accumulator.release();
        if (socket != null) {
            return socket.close();
        }
        return Future.succeededFuture();
    }

    // -----------------------------------------------------------------------
    // Handshake state machine
    // -----------------------------------------------------------------------

    /**
     * State used during the INIT/OPEN handshake.
     * After the handshake the promise is completed and normal message routing begins.
     */
    private enum HandshakeState { WAIT_INIT_ACK, WAIT_OPEN_ACK, DONE }
    private volatile HandshakeState handshakeState = HandshakeState.WAIT_INIT_ACK;
    private volatile byte[] pendingCookie;

    /**
     * Called by the Vert.x socket handler for every chunk of data received.
     * Handles both the handshake phase and the steady-state frame dispatching.
     */
    private void handleData(Buffer chunk, Promise<Void> handshakePromise) {
        accumulator.writeBytes(chunk.getBytes());

        while (true) {
            if (accumulator.readableBytes() < 2) break;

            // Peek at the 2-byte LE length prefix
            int savedIndex = accumulator.readerIndex();
            int batchLen = accumulator.readUnsignedByte() | (accumulator.readUnsignedByte() << 8);

            if (accumulator.readableBytes() < batchLen) {
                // Not enough data yet; restore the index and wait
                accumulator.readerIndex(savedIndex);
                break;
            }

            // We have a complete batch — slice it and decode
            ByteBuf batch = accumulator.readSlice(batchLen);
            processBatch(batch, handshakePromise);
        }

        // Compact the accumulator to reclaim already-read bytes
        accumulator.discardReadBytes();
    }

    private void processBatch(ByteBuf batch, Promise<Void> handshakePromise) {
        while (batch.isReadable()) {
            Object msg = ZenohDecoder.decodeTransportMessage(batch);
            if (msg == null) break;

            switch (handshakeState) {
                case WAIT_INIT_ACK -> {
                    if (msg instanceof InitAck ack) {
                        pendingCookie = ack.cookie();
                        // Send OpenSyn
                        long initialSn = (long) (Math.random() * Integer.MAX_VALUE);
                        OpenSyn openSyn = new OpenSyn(10_000L, initialSn, pendingCookie);
                        ByteBuf encoded = ZenohEncoder.encodeOpenSyn(openSyn);
                        sendRaw(encoded);
                        handshakeState = HandshakeState.WAIT_OPEN_ACK;
                    } else {
                        log.warn("Expected InitAck, got {}", msg.getClass().getSimpleName());
                    }
                }
                case WAIT_OPEN_ACK -> {
                    if (msg instanceof OpenAck) {
                        handshakeState = HandshakeState.DONE;
                        connected.set(true);
                        handshakePromise.complete();
                    } else if (msg instanceof Close c) {
                        handshakePromise.fail("Router closed during handshake, reason=" + c.reason());
                    } else {
                        log.warn("Expected OpenAck, got {}", msg.getClass().getSimpleName());
                    }
                }
                case DONE -> dispatchMessage(msg);
            }
        }
    }

    private void dispatchMessage(Object msg) {
        if (msg instanceof ZenohDecoder.DecodedFrame frame) {
            for (Object netMsg : frame.messages()) {
                try {
                    messageHandler.accept(netMsg);
                } catch (Exception e) {
                    log.error("Error in message handler", e);
                }
            }
        } else if (msg instanceof KeepAlive) {
            // respond with keepalive
            sendRaw(ZenohEncoder.encodeKeepAlive());
        } else if (msg instanceof Close) {
            log.info("Received Close from router");
            connected.set(false);
            closeHandler.run();
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void sendRaw(ByteBuf buf) {
        if (socket == null) {
            buf.release();
            return;
        }
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        buf.release();
        socket.write(Buffer.buffer(bytes));
    }

    /** Generates a random 16-byte Zenoh ID. */
    private static byte[] generateZid() {
        UUID uuid = UUID.randomUUID();
        ByteBuf buf = Unpooled.buffer(16);
        buf.writeLong(uuid.getMostSignificantBits());
        buf.writeLong(uuid.getLeastSignificantBits());
        byte[] zid = new byte[16];
        buf.readBytes(zid);
        buf.release();
        return zid;
    }
}
