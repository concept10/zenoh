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

import java.util.Objects;

/**
 * The encoding metadata associated with a Zenoh payload.
 * <p>
 * An encoding is defined by a numeric {@code id} (matching the zenoh encoding table)
 * and an optional string {@code schema} for finer-grained description.
 *
 * @see <a href="https://github.com/eclipse-zenoh/zenoh/blob/main/commons/zenoh-protocol/src/core/encoding.rs">encoding.rs</a>
 */
public final class Encoding {

    // -----------------------------------------------------------------------
    // Well-known encoding IDs (from zenoh-protocol/src/core/encoding.rs)
    // -----------------------------------------------------------------------
    public static final Encoding ZENOH_BYTES        = new Encoding(0,   null);
    public static final Encoding ZENOH_INT8         = new Encoding(1,   null);
    public static final Encoding ZENOH_INT16        = new Encoding(2,   null);
    public static final Encoding ZENOH_INT32        = new Encoding(3,   null);
    public static final Encoding ZENOH_INT64        = new Encoding(4,   null);
    public static final Encoding ZENOH_INT128       = new Encoding(5,   null);
    public static final Encoding ZENOH_UINT8        = new Encoding(6,   null);
    public static final Encoding ZENOH_UINT16       = new Encoding(7,   null);
    public static final Encoding ZENOH_UINT32       = new Encoding(8,   null);
    public static final Encoding ZENOH_UINT64       = new Encoding(9,   null);
    public static final Encoding ZENOH_UINT128      = new Encoding(10,  null);
    public static final Encoding ZENOH_FLOAT32      = new Encoding(11,  null);
    public static final Encoding ZENOH_FLOAT64      = new Encoding(12,  null);
    public static final Encoding ZENOH_BOOL         = new Encoding(13,  null);
    public static final Encoding ZENOH_STRING       = new Encoding(14,  null);
    public static final Encoding ZENOH_ERROR        = new Encoding(15,  null);
    public static final Encoding APPLICATION_OCTET_STREAM = new Encoding(16, null);
    public static final Encoding TEXT_PLAIN         = new Encoding(31,  null);
    public static final Encoding APPLICATION_JSON   = new Encoding(22,  null);
    public static final Encoding TEXT_JSON          = new Encoding(23,  null);
    public static final Encoding TEXT_CSV           = new Encoding(24,  null);
    public static final Encoding TEXT_HTML          = new Encoding(25,  null);
    public static final Encoding TEXT_XML           = new Encoding(26,  null);

    // -----------------------------------------------------------------------

    private final int id;
    private final String schema;

    public Encoding(int id, String schema) {
        this.id = id;
        this.schema = schema;
    }

    /** Returns the numeric encoding ID. */
    public int id() {
        return id;
    }

    /** Returns the optional schema string, or {@code null}. */
    public String schema() {
        return schema;
    }

    /** Returns an {@link Encoding} with the given schema appended. */
    public Encoding withSchema(String schema) {
        return new Encoding(this.id, schema);
    }

    /** Returns the empty/default encoding (id=0, no schema). */
    public static Encoding empty() {
        return ZENOH_BYTES;
    }

    @Override
    public String toString() {
        return schema != null ? id + ";" + schema : String.valueOf(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Encoding)) return false;
        Encoding e = (Encoding) o;
        return id == e.id && Objects.equals(schema, e.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, schema);
    }
}
