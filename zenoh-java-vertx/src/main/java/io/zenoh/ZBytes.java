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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * An opaque payload of bytes used by Zenoh messages.
 * <p>
 * {@code ZBytes} wraps a raw {@code byte[]} and provides helpers
 * for converting to/from common types.
 */
public final class ZBytes {

    private final byte[] bytes;

    private ZBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    /** Creates a {@link ZBytes} wrapping a copy of the given bytes. */
    public static ZBytes of(byte[] bytes) {
        return new ZBytes(Arrays.copyOf(bytes, bytes.length));
    }

    /** Creates a {@link ZBytes} from a UTF-8 string. */
    public static ZBytes of(String text) {
        return new ZBytes(text.getBytes(StandardCharsets.UTF_8));
    }

    /** Creates an empty {@link ZBytes}. */
    public static ZBytes empty() {
        return new ZBytes(new byte[0]);
    }

    /**
     * Returns the raw bytes. The returned array is a direct reference;
     * callers must not modify it.
     */
    public byte[] rawBytes() {
        return bytes;
    }

    /** Returns the number of bytes. */
    public int length() {
        return bytes.length;
    }

    /** Converts the bytes to a UTF-8 string. */
    public String tryToString() {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Returns {@code true} if this payload contains no bytes. */
    public boolean isEmpty() {
        return bytes.length == 0;
    }

    @Override
    public String toString() {
        return tryToString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ZBytes)) return false;
        return Arrays.equals(bytes, ((ZBytes) o).bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}
