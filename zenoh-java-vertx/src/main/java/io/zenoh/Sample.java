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

/**
 * A data sample received from the Zenoh network.
 * <p>
 * A sample carries a {@link KeyExpr}, a {@link ZBytes} payload, an {@link Encoding},
 * and the {@link SampleKind} (PUT or DELETE).
 */
public final class Sample {

    private final KeyExpr keyExpr;
    private final ZBytes payload;
    private final Encoding encoding;
    private final SampleKind kind;

    public Sample(KeyExpr keyExpr, ZBytes payload, Encoding encoding, SampleKind kind) {
        this.keyExpr = keyExpr;
        this.payload = payload;
        this.encoding = encoding;
        this.kind = kind;
    }

    /** The key expression for this sample. */
    public KeyExpr keyExpr() {
        return keyExpr;
    }

    /** The payload bytes of this sample. */
    public ZBytes payload() {
        return payload;
    }

    /** The encoding metadata of the payload. */
    public Encoding encoding() {
        return encoding;
    }

    /** Whether this is a {@link SampleKind#PUT} or {@link SampleKind#DELETE}. */
    public SampleKind kind() {
        return kind;
    }

    @Override
    public String toString() {
        return "Sample{key=" + keyExpr + ", kind=" + kind + ", encoding=" + encoding
                + ", payload=" + payload + "}";
    }
}
