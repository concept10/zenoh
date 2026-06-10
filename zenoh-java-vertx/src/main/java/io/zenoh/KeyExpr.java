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
 * A Zenoh key expression.
 * <p>
 * Key expressions are hierarchical resource paths (e.g., {@code "demo/example/hello"})
 * with support for wildcards ({@code *}, {@code **}).
 */
public final class KeyExpr {

    private final String expr;

    private KeyExpr(String expr) {
        if (expr == null || expr.isEmpty()) {
            throw new IllegalArgumentException("Key expression must not be null or empty");
        }
        this.expr = expr;
    }

    /**
     * Creates a {@link KeyExpr} from a string.
     *
     * @param expr the key expression string (e.g., {@code "demo/example/**"})
     * @return a new {@link KeyExpr}
     */
    public static KeyExpr of(String expr) {
        return new KeyExpr(expr);
    }

    /** Returns the string representation of this key expression. */
    public String asStr() {
        return expr;
    }

    /**
     * Returns {@code true} if this key expression intersects with {@code other}.
     * Two key expressions intersect if there is at least one resource that matches both.
     */
    public boolean intersects(KeyExpr other) {
        return intersects(this.expr, other.expr);
    }

    private static boolean intersects(String a, String b) {
        return matchWildcard(a, b) || matchWildcard(b, a);
    }

    private static boolean matchWildcard(String pattern, String key) {
        String[] pParts = pattern.split("/", -1);
        String[] kParts = key.split("/", -1);
        return match(pParts, 0, kParts, 0);
    }

    private static boolean match(String[] p, int pi, String[] k, int ki) {
        while (pi < p.length && ki < k.length) {
            String ps = p[pi];
            if ("**".equals(ps)) {
                // ** matches zero or more path segments
                for (int i = ki; i <= k.length; i++) {
                    if (match(p, pi + 1, k, i)) {
                        return true;
                    }
                }
                return false;
            } else if ("*".equals(ps)) {
                pi++;
                ki++;
            } else if (ps.equals(k[ki])) {
                pi++;
                ki++;
            } else {
                return false;
            }
        }
        // Consume trailing **
        while (pi < p.length && "**".equals(p[pi])) {
            pi++;
        }
        return pi == p.length && ki == k.length;
    }

    @Override
    public String toString() {
        return expr;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KeyExpr)) return false;
        return expr.equals(((KeyExpr) o).expr);
    }

    @Override
    public int hashCode() {
        return expr.hashCode();
    }
}
