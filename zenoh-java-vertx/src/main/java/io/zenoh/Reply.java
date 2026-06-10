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
 * A reply received from a {@link Session#get} query.
 *
 * <p>A reply is either a successful {@link Sample} or an error payload.
 * Check {@link #isOk()} before accessing {@link #getSample()}.
 */
public final class Reply {

    private final Sample sample;
    private final ZBytes error;

    private Reply(Sample sample, ZBytes error) {
        this.sample = sample;
        this.error  = error;
    }

    /** Creates a successful reply. */
    public static Reply ok(Sample sample) {
        return new Reply(sample, null);
    }

    /** Creates an error reply. */
    public static Reply error(ZBytes error) {
        return new Reply(null, error);
    }

    /** Returns {@code true} if this reply carries a successful sample. */
    public boolean isOk() {
        return sample != null;
    }

    /**
     * Returns the successful {@link Sample}.
     *
     * @throws IllegalStateException if {@link #isOk()} is {@code false}
     */
    public Sample getSample() {
        if (sample == null) throw new IllegalStateException("Reply is an error; use getError()");
        return sample;
    }

    /**
     * Returns the error payload.
     *
     * @throws IllegalStateException if {@link #isOk()} is {@code true}
     */
    public ZBytes getError() {
        if (error == null) throw new IllegalStateException("Reply is successful; use getSample()");
        return error;
    }

    @Override
    public String toString() {
        return isOk() ? "Reply{ok=" + sample + "}" : "Reply{error=" + error + "}";
    }
}
