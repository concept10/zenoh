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
 * Congestion control behaviour when a Zenoh network is congested.
 * <ul>
 *   <li>{@link #DROP} – discard the message rather than block.</li>
 *   <li>{@link #BLOCK} – block the publisher until the message can be sent.</li>
 * </ul>
 */
public enum CongestionControl {
    DROP,
    BLOCK;

    /** Returns the default congestion control ({@link #DROP}). */
    public static CongestionControl defaultControl() {
        return DROP;
    }
}
