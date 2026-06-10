# zenoh-java-vertx

A native Java implementation of the [Zenoh](https://zenoh.io) protocol using [Eclipse Vert.x](https://vertx.io) for async I/O.

This module implements the **Zenoh 0.8 wire protocol** from scratch in pure Java, speaking directly to a [zenoh-router](https://zenoh.io/docs/getting-started/first-app/) over TCP — no JNI, no Rust bindings required.

---

## Architecture

```
┌────────────────────────────────────────────┐
│               Public API                   │
│  Zenoh · Session · Publisher · Subscriber  │
│  Queryable · Query · Reply · Sample        │
├────────────────────────────────────────────┤
│             Protocol Codec                 │
│   ZenohEncoder · ZenohDecoder · VarInt     │
├────────────────────────────────────────────┤
│           Transport (Vert.x TCP)           │
│   ZenohTransport (INIT/OPEN handshake,     │
│   2-byte LE batch framing, LEB128 VarInt)  │
└────────────────────────────────────────────┘
```

**Key design choices:**
- All network I/O is **non-blocking** via Vert.x `NetClient` / `NetSocket`.
- Encoding uses [Netty `ByteBuf`](https://netty.io/wiki/reference-counted-objects.html) (included transitively via Vert.x).
- The wire format follows the Zenoh 0.8 spec: LEB128 VarInt, 2-byte LE length-prefixed batches, Init/Open handshake.

---

## Requirements

| Tool | Version |
|------|---------|
| Java | 17+     |
| Maven | 3.8+   |
| zenoh-router | 0.8.x (for integration tests / examples) |

---

## Build

```bash
cd zenoh-java-vertx
mvn package -DskipTests
```

A fat JAR is produced at `target/zenoh-java-vertx-1.0.0-fat.jar`.

Run all unit tests (no router needed):

```bash
mvn test
```

Run integration tests (requires a running zenoh-router on `localhost:7447`):

```bash
zenohd &   # start the router
mvn test -Dzenoh.test.router=true
```

---

## Quick start

### 1. Open a session

```java
Vertx vertx = Vertx.vertx();
Session session = Zenoh.open(Config.defaultConfig(), vertx)
        .toCompletionStage().toCompletableFuture().get();
```

Custom router address:

```java
Config config = Config.builder()
        .connectEndpoint("tcp/192.168.1.100:7447")
        .build();
Session session = Zenoh.open(config, vertx)
        .toCompletionStage().toCompletableFuture().get();
```

### 2. Publish

```java
// One-shot put
session.put(KeyExpr.of("demo/hello"), ZBytes.of("World"));

// Declared publisher (more efficient for repeated publishes)
Publisher pub = session.declarePublisher(KeyExpr.of("demo/counter"));
pub.put(ZBytes.of("42"));
pub.close();
```

### 3. Subscribe

```java
Subscriber sub = session.declareSubscriber(
        KeyExpr.of("demo/**"),
        sample -> System.out.printf("[%s] %s%n", sample.keyExpr(), sample.payload())
).toCompletionStage().toCompletableFuture().get();

// ... later
sub.close();
```

### 4. Get (query)

```java
session.get(
        KeyExpr.of("demo/**"),
        reply -> {
            if (reply.isOk()) System.out.println(reply.getSample().payload());
            else System.err.println("Error: " + reply.getError());
        },
        5000 /* timeout ms */
).toCompletionStage().toCompletableFuture().get();
```

### 5. Queryable

```java
Queryable qbl = session.declareQueryable(
        KeyExpr.of("demo/sensor"),
        query -> query.reply(query.keyExpr(), ZBytes.of("temperature=22.5"))
).toCompletionStage().toCompletableFuture().get();

// ... later
qbl.close();
```

---

## Examples

Start a zenoh-router first:

```bash
zenohd
```

Then run any example via Maven exec or the fat JAR:

| Example | Description |
|---------|-------------|
| `ZPub` | Publish a counter value every second |
| `ZSub` | Subscribe and print received samples |
| `ZPut` | Publish a single value and exit |
| `ZGet` | Query and print all replies |
| `ZQueryable` | Serve queries with a static reply |

```bash
# ZPub  – publish 10 messages on demo/example/vertx-zpub
mvn exec:java -Dexec.mainClass=io.zenoh.examples.ZPub -Dexec.args="demo/example/vertx-zpub 10"

# ZSub  – subscribe on demo/example/**
mvn exec:java -Dexec.mainClass=io.zenoh.examples.ZSub

# ZGet  – query demo/example/**
mvn exec:java -Dexec.mainClass=io.zenoh.examples.ZGet
```

Or with the fat JAR:

```bash
java -cp target/zenoh-java-vertx-1.0.0-fat.jar io.zenoh.examples.ZPub
```

---

## Project structure

```
zenoh-java-vertx/
├── pom.xml
└── src/
    ├── main/java/io/zenoh/
    │   ├── Zenoh.java            # static open() entry point
    │   ├── Session.java          # main API: put/subscribe/get/queryable
    │   ├── Publisher.java        # declared publisher
    │   ├── Subscriber.java       # declared subscriber
    │   ├── Queryable.java        # declared queryable
    │   ├── Query.java            # incoming query + reply methods
    │   ├── Reply.java            # reply to a get query (ok or error)
    │   ├── Sample.java           # received data sample
    │   ├── KeyExpr.java          # key expression with wildcard intersection
    │   ├── ZBytes.java           # opaque byte payload
    │   ├── Encoding.java         # encoding metadata + well-known constants
    │   ├── Config.java           # session configuration builder
    │   ├── SampleKind.java       # PUT / DELETE enum
    │   ├── Priority.java         # QoS priority (1–7)
    │   ├── CongestionControl.java
    │   ├── examples/
    │   │   ├── ZPub.java
    │   │   ├── ZSub.java
    │   │   ├── ZPut.java
    │   │   ├── ZGet.java
    │   │   └── ZQueryable.java
    │   └── internal/
    │       ├── ZenohTransport.java       # Vert.x TCP + INIT/OPEN handshake
    │       ├── codec/
    │       │   ├── VarInt.java           # LEB128 encoder/decoder
    │       │   ├── ZenohEncoder.java     # message → ByteBuf
    │       │   └── ZenohDecoder.java     # ByteBuf → message
    │       └── messages/
    │           └── Messages.java         # protocol constants + record types
    └── test/java/io/zenoh/
        ├── CodecTest.java        # unit tests (no router needed)
        └── SessionTest.java      # integration tests (requires router)
```

---

## Dependencies

| Artifact | Version | Purpose |
|----------|---------|---------|
| `io.vertx:vertx-core` | 4.5.10 | Async TCP I/O |
| `io.vertx:vertx-rx-java3` | 4.5.10 | RxJava3 Future adapters |
| `io.netty:netty-buffer` | (via Vert.x) | `ByteBuf` for zero-copy encoding |
| `org.slf4j:slf4j-api` | 2.0.12 | Logging facade |
| `ch.qos.logback:logback-classic` | 1.5.6 | Default logging backend |
| `org.junit.jupiter:junit-jupiter` | 5.10.2 | Test framework |

---

## Protocol notes

The implementation follows the **Zenoh 0.8** wire protocol:

- **Transport layer**: TCP with 2-byte little-endian length-prefixed batches.
- **VarInt**: standard LEB128 (7 data bits per byte, MSB = continuation).
- **Handshake**: `InitSyn → InitAck → OpenSyn → OpenAck` four-way INIT/OPEN.
- **Network messages**: `Push` (pub/sub), `Request`/`Response`/`ResponseFinal` (query), `Declare` (subscriptions/queryables), `Interest` (resource announcements).
- **WireExpr**: `(scope: u16, suffix: optional u16-bounded string)` — scope 0 means full key in suffix.
- **Encoding**: shifted u32 where bit 0 = has-schema, remaining bits = encoding ID.

---

## License

Apache 2.0 / Eclipse Public License 2.0 — see [LICENSE](../LICENSE).
