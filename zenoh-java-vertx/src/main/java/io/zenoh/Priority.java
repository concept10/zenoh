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
 * QoS priority for Zenoh messages.
 * Lower numeric values indicate higher priority.
 */
public enum Priority {
    REAL_TIME(1),
    INTERACTIVE_HIGH(2),
    INTERACTIVE_LOW(3),
    DATA_HIGH(4),
    DATA(5),
    DATA_LOW(6),
    BACKGROUND(7);

    private final int value;

    Priority(int value) {
        this.value = value;
    }

    /** Returns the wire-level numeric value (1–7). */
    public int value() {
        return value;
    }

    /** Returns the default priority ({@link #DATA}). */
    public static Priority defaultPriority() {
        return DATA;
    }

    /** Converts a numeric value to a {@link Priority}. */
    public static Priority fromValue(int v) {
        for (Priority p : values()) {
            if (p.value == v) return p;
        }
        return DATA;
    }
}
