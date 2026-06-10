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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Configuration for a Zenoh {@link Session}.
 * <p>
 * Use the {@link Builder} to construct instances.
 *
 * <pre>{@code
 * Config config = Config.builder()
 *     .connect("tcp/localhost:7447")
 *     .mode(Config.Mode.CLIENT)
 *     .build();
 * }</pre>
 */
public final class Config {

    /** The role of the Zenoh node. */
    public enum Mode {
        CLIENT,
        PEER,
        ROUTER
    }

    private final Mode mode;
    private final List<String> connectEndpoints;
    private final List<String> listenEndpoints;

    private Config(Builder builder) {
        this.mode = builder.mode;
        this.connectEndpoints = Collections.unmodifiableList(new ArrayList<>(builder.connectEndpoints));
        this.listenEndpoints = Collections.unmodifiableList(new ArrayList<>(builder.listenEndpoints));
    }

    /** Returns the operating mode. */
    public Mode mode() {
        return mode;
    }

    /** Returns the list of endpoints to connect to. */
    public List<String> connectEndpoints() {
        return connectEndpoints;
    }

    /** Returns the list of endpoints to listen on. */
    public List<String> listenEndpoints() {
        return listenEndpoints;
    }

    /** Returns a default configuration (CLIENT mode, connects to {@code tcp/localhost:7447}). */
    public static Config defaultConfig() {
        return builder().build();
    }

    /** Creates a new {@link Builder}. */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "Config{mode=" + mode + ", connect=" + connectEndpoints + "}";
    }

    // -----------------------------------------------------------------------

    public static final class Builder {
        private Mode mode = Mode.CLIENT;
        private final List<String> connectEndpoints = new ArrayList<>();
        private final List<String> listenEndpoints = new ArrayList<>();

        private Builder() {
            // default connect endpoint
            connectEndpoints.add("tcp/localhost:7447");
        }

        /** Sets the operating mode. */
        public Builder mode(Mode mode) {
            this.mode = mode;
            return this;
        }

        /**
         * Replaces all connect endpoints with the given single endpoint.
         * Format: {@code "tcp/<host>:<port>"} or {@code "udp/<host>:<port>"}.
         */
        public Builder connect(String endpoint) {
            this.connectEndpoints.clear();
            this.connectEndpoints.add(endpoint);
            return this;
        }

        /** Adds a connect endpoint. */
        public Builder addConnect(String endpoint) {
            this.connectEndpoints.add(endpoint);
            return this;
        }

        /** Adds a listen endpoint. */
        public Builder listen(String endpoint) {
            this.listenEndpoints.add(endpoint);
            return this;
        }

        /** Builds the {@link Config}. */
        public Config build() {
            return new Config(this);
        }
    }
}
