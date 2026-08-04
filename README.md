# jolt-tcp

A [teensyp](https://github.com/weavejester/teensyp)-compatible TCP server for
**[jolt](https://github.com/jolt-lang/jolt)** (Clojure on Chez Scheme) — no JVM,
no Java NIO. It runs a single **`jolt.net` readiness reactor** plus handler and
callback pools, and mirrors the `teensyp.*` namespaces so teensyp-style handlers
run unmodified.

This is to teensyp what
[ring-chez-adapter](https://github.com/jolt-lang/ring-chez-adapter) is to Ring:
a native reimplementation of the server contract on jolt's owned sockets.

## Usage

```clojure
(require '[teensyp.server :as tcp]
         '[teensyp.buffer :as buf])

(defn echo-handler
  ([sock] {:bytes 0})                          ;; accept: initial per-conn state
  ([state sock b]                              ;; read: b is a teensyp.buffer/Buffer
   (let [n (buf/remaining b)]
     (when (pos? n)
       (tcp/write sock (buf/wrap (buf/get-bytes! b n))))  ;; consume + echo
     (update state :bytes + n)))
  ([state ex] (println "closed, bytes =" (:bytes state))))  ;; close: ex nil if graceful

(def server (tcp/run-server :port 3000 :handler echo-handler :reuse-address? true))
;; ... later ...
(tcp/stop-server server)   ;; also works with (with-open [s (tcp/run-server ...)] ...)
```

The handler is one function with three arities — **accept**, **read**, **close**
— threading a per-connection `state` value, exactly as in JVM teensyp. Calls are
serial per connection: accept first, close last, reads sequential, and the write
queue is drained before the next read.

## API

### `teensyp.client`

A production blocking outbound client over `jolt.net`:

- `(connect host port)` / `(connect host port opts)` and
  `(connect-endpoint endpoint opts)` resolve once and try candidates in resolver
  order. `:connect-timeout-ms` defaults to 30000; explicitly passing
  `:connect-timeout-ms nil` selects an unbounded connect. Callers that already
  compose deadlines may pass one absolute `:deadline-nanos` in
  `jolt.host/mono-nanos` units. The one deadline covers resolution and
  every candidate rather than restarting for each address. The current
  `jolt.net` resolver is synchronous: an over-deadline result is rejected, but
  an in-flight `getaddrinfo` cannot yet be preempted.
- `(send-all! connection bytes)` and its slice arity block until every byte is
  written, correctly advancing across partial writes and retrying
  the `jolt.net/would-block` sentinel. Both forms accept an additive options
  arity with either relative `:timeout-ms` or absolute `:deadline-nanos`;
  omitting both remains unbounded.
- `(receive-into! connection bytes off len)` returns a positive count, zero for
  an empty slice, or nil at EOF. `(receive-at-most! connection n)` provides the
  allocating convenience form. Both receive forms accept the same optional
  deadline map.
- `(shutdown-write! connection)` half-closes once while preserving reads.
  `(close! connection)` is idempotent, and the connection also works with
  `with-open`.
- `(connection-info connection)` returns transport-neutral local/remote
  endpoints and lifecycle state, never a socket, poller, or native descriptor.

Each connection owns persistent read and write pollers. Same-direction
operations are FIFO-serialized by an owner-independent promise gate, while one
reader and one writer may proceed concurrently. Closing wakes both directions
before retiring the socket; no operation allocates a poller. Operation
deadlines are computed at API entry and include time queued behind an earlier
same-direction operation. A timed-out queue ticket is cancelled without taking
ownership. Deadline expiry throws `:teensyp.client/timed-out` with
`:teensyp.client/op` set to `:send` or `:receive`; EOF and native reset/close
remain their original transport outcomes.

### `teensyp.server`
- `(run-server & opts)` / `(run-server opts-map)` — start the server; returns a
  handle usable with `stop-server` and `with-open`. Options: `:port` (required),
  `:handler` (required), `:read-buffer-size` (8192), `:write-buffer-size`
  (32768), `:write-queue-size` (64), `:control-queue-size` (32),
  `:accept-batch-size` (64), `:reuse-address?`, `:recv-buffer-size`,
  `:executor`, `:pool-size` (4), `:callback-executor`,
  `:callback-pool-size` (2), `:shutdown-executor?` (false),
  `:shutdown-callback-executor?` (false), `:error-logger`, and
  `:stop-timeout-ms` (5000).
  - `:error-logger` is called on a reactor-side error. The offending connection
    is closed, but the reactor keeps running — one bad connection never takes
    down the server.
  - `:callback-executor` runs write/control completion callbacks, and is
    deliberately separate from `:executor`. A handler may block until one of its
    writes completes (that is how a blocking sink over a socket is built); if the
    releasing callback shared the handler pool, enough concurrent blocked
    handlers would deadlock at exactly `pool-size`. Passing the same executor
    object for both roles is rejected. The default callback executor is a small,
    configurable fixed pool rather than an eager cached pool.
  - `:accept-batch-size` bounds the work one listener-readiness turn can perform.
    Stop is checked between accepts, and any accepted socket that fails
    registration or loses cancellation before publication is closed and its
    registration rolled back.
  - Supplied `:executor` and `:callback-executor` values are borrowed by default
    and remain usable after the server stops. Pools created by the server are
    always shut down. Set `:shutdown-executor? true` and/or
    `:shutdown-callback-executor? true` to let the server adopt and shut down a
    supplied pool, including cleanup after a failed start.
- `(stop-server server)` — stop accepting, quiesce active handlers, retire every
  registration and owned socket, then run and await each open connection's
  close-arity before closing the poller and owned executors. While quiescing,
  the reactor continues draining writes and control callbacks required by
  already-active handlers, but accepts no connections and dispatches no new
  reads. Repeated calls are idempotent. A cleanup timeout throws structured
  `ExceptionInfo` with `:err :teensyp.server/stop-timeout`; cleanup continues,
  and a later call waits on the same completion.
- `(write sock buffer)` / `(write sock buffer callback)` — queue a Buffer; the
  zero-arg callback fires once fully written and intentionally remains
  success-only.
- `(write-completion sock buffer)` — queue a Buffer and return a promise that
  settles with `{:status :written}` after full success or
  `{:status :failed :exception ex}` on connection/write failure. Queue-capacity
  errors remain synchronous. A synchronous `::socket-closed` admission error
  retains the first recorded connection failure as its `ex-cause`. Blocking
  adapters should use this API so a native write error cannot strand a callback
  waiter.
- `(close sock)` / `(close sock callback)` — queue the socket to close.
- `(pause-reads sock)` / `(resume-reads sock)` — backpressure controls.
- `(peer-closed? sock)` — true once the peer has half-closed its write side, so
  no more data will arrive. **The connection is not closed for you.** The peer
  may still be reading and is usually owed a response, and the reactor cannot
  tell when one is still coming (a handler on a worker thread raises no flag),
  so the handler owns the close. The read arity is called once when this becomes
  true, possibly with an empty buffer.
- `Socket` protocol: `try-write`, `queue-control`, `queue-write`, `socket-info`,
  `socket-lock`. The additive `CompletionSocket` protocol supplies
  `queue-write-completion` without changing existing `Socket` implementations.

### `teensyp.buffer`
A byte-array-backed `ByteBuffer` equivalent (jolt has no `java.nio.ByteBuffer`):
`buffer`, `wrap`, `str->buffer`, `buffer->str`, `remaining`, `has-remaining?`,
`position`/`set-position!`, `limit`/`set-limit!`, `flip`, `compact`, `duplicate`,
`index-of`, `index-of-array`, `read-line`, `copy`, `get-bytes!`, `put-bytes!`.
Charset arguments are **name strings** (e.g. `"UTF-8"`), since jolt has no
`java.nio.charset.Charset`.

### `teensyp.stream`
A jolt-native blocking-connection adapter (jolt cannot subclass
`java.io.InputStream`/`OutputStream`). `stream-handler` runs `(f conn)` on its
own thread, where `conn` supports `conn-recv`, `conn-read-line`, `conn-send`,
`conn-close`, backed by a core.async channel the reader fills. `conn-send`
blocks until the write outcome is known and throws the recorded native
exception on failure.

### `teensyp.ffi-net`
A compatibility shim for the former real-loopback helper. Despite the legacy
namespace name, it has no FFI bindings or raw-descriptor API: all operations
delegate to the production `teensyp.client` surface.

## Differences from JVM teensyp

- **Transport**: a `jolt.net` poller instead of an NIO `Selector`; all socket I/O
  and registration mutation runs on the reactor thread, so `try-write` always
  queues (returns false). The completion callback and queue limits are unchanged.
- **Buffer**: `teensyp.buffer/Buffer` (byte-array backed) in place of
  `java.nio.ByteBuffer`; consume by advancing `position`, exactly as before.
  Handlers using the `teensyp.buffer` functions are drop-in; handlers that called
  raw `.position`/`.remaining` Java interop switch to `buf/position`/`buf/remaining`.
- **Charsets**: name strings, not `Charset` objects.
- **Streams**: `teensyp.stream` exposes a `conn` protocol, not `java.io` streams.
- **socket-info**: transport-neutral numeric endpoint maps
  `{:host string :port integer :family keyword}` in `:local-address` and
  `:remote-address`, plus `:fd` for diagnostics only. The public TCP boundary
  does not expose `jolt.net`-namespaced data keys.
- **Platform**: follows the readiness targets implemented by the pinned
  `jolt.net`: Linux x86_64, Linux aarch64, macOS arm64, macOS x86_64, Windows
  x86_64, and Windows aarch64. That is the underlying transport capability;
  this repository's narrower per-platform Jolt-TCP evidence is listed below.

### CI platform coverage

- Linux x86_64 and macOS arm64 have observed native runtime evidence for the
  complete reactor, real-loopback acceptance, outbound-client, and property
  suites with source-built Chez 10.4.1.
- Linux aarch64 (`ubuntu-24.04-arm`) and macOS x86_64
  (`macos-15-intel`) have equivalent full-runtime jobs configured. Treat both
  as candidate coverage until those jobs are observed green on this revision.
  Because libhegel 0.30.1 publishes no Darwin/x86_64 asset, the Intel job builds
  its exact tagged source and supplies the resulting library explicitly.
- Windows x86_64 has candidate native layered gates. The first source-builds
  Chez, loads the production TCP namespaces, opens and closes the public
  WSAPoll-backed poller, and runs every outbound-client model including a real
  server/client loopback with no optional test dependency. The second installs
  libhegel and runs the buffer properties. This is client/socket-runtime
  evidence, not yet the complete server acceptance and stress suite.
- Windows aarch64 has a non-gating public-preview source-mode lane on
  `windows-11-vs2026-arm`. It builds native `tarm64nt` Chez 10.4.1 and runs the
  `teensyp.buffer` property suite through the upstream Windows ARM64 libhegel
  asset. It also requires the probed ARM64 descriptor, loads the production
  client/server namespaces, and opens and closes the public poller. It does not
  yet run the full TCP loopback or server acceptance suite and uses no packaged
  joltc, devboot, or AOT cache.

## Testing

```sh
JOLT_PWD="$PWD" /path/to/reviewed-jolt/bin/joltc \
  -A:test -m hegel.install
JOLT_HEGEL_REQUIRED=1 JOLT_PWD="$PWD" \
  /path/to/reviewed-jolt/bin/joltc -M:test
```

The reviewed Jolt core is commit
`9fc64f93eba8b56a319f91bb1a322e2efced9c70`; `deps.edn` pins the reviewed
cross-platform jolt-net aggregate at
`699b908ffb4eb79ad35055cdc20866bb504e6932`. The required-mode environment
variable prevents a missing or unloadable libhegel from silently skipping the
property layers.

Three layers, all gated by the single `-M:test` command, which exits
non-zero on any failure.

**Acceptance** (`teensyp.server-test`) — the framework-less harness. Starts
servers on loopback and drives echo, line-reverse (with split frames),
number-doubling, write-then-close, write-callback ordering, pause/resume
backpressure, the stream layer, and 12 concurrent connections over real TCP.
Deterministic lifecycle regressions additionally cover active-only task
tracking, fixed callback-pool bounds and executor separation, CAS queue caps and
byte-credit rollback, atomic close exceptions, bounded/cancelled accept
rollback, CLOSED readiness clearing, and success/failure write outcomes.

**Buffer properties** (`teensyp.buffer-property-test`) — generative properties for
`teensyp.buffer` via [jolt-hegel](https://github.com/chucklehead-dev/jolt-hegel),
run under `clojure.test`. Round trips, the `0 <= position <= limit <= capacity`
invariant, `compact`/`copy`/`duplicate` conservation, and agreement between
`index-of`/`index-of-array`/`read-line` and independent naive implementations —
plus a stateful model replaying generated operation sequences against a Buffer,
mirroring the server's read path.

**TCP properties** (`teensyp.server-property-test`) — properties over real
loopback connections, using the size-based chunking generator so the engine picks
the split points (the acceptance suite pins one):

- echo conservation under arbitrary chunking, with `:read-buffer-size 64` so
  ordinary payloads repeatedly overflow the read buffer and exercise compaction,
  the `FULL` flag and the resume path;
- line framing invariance under arbitrary chunking;
- chained write-callback ordering;
- pause/resume backpressure losing nothing;
- a single large write, driving partial-send/would-block handling;
- `teensyp.stream` line framing under arbitrary chunking.

Every case ends in a half-close and drains to a real EOF — bounded in time, never
slept on — so the EOF path is covered on every case. The stream property was
promoted into the default gate after 30 consecutive 25-case stress runs
completed cleanly; `run-stream-property!` remains available for focused stress.

The bounded [reactor lifecycle
proofs](docs/proofs/reactor-lifecycle-invariants.md) preserve the
ownership/generation boundary delegated to `jolt.net`, plus the EOF
byte-visibility, recursive-lock, admission, shutdown, and write-outcome
arguments together with their SAT/UNSAT controls and executable runtime
witnesses. The separate [outbound-client proof
record](docs/proofs/client-connection-invariants.md) covers absolute deadline
composition, resolver-candidate/socket/poller ownership, partial byte I/O, and
the public loopback contract.

jolt-hegel is a test-only dependency, pinned by SHA in the `:test` alias.
Property failures print a replayable seed.

> **jolt AOT cache caveat.** Released jolt's namespace cache can run stale code
> or omit top-level initialization effects. Run development and validation with
> `JOLT_AOT_CACHE=0`; the reviewed fork's launcher already defaults to that
> fail-safe setting. Namespace-level runtime reuse is not a supported correctness
> boundary for this project.

## License

EPL-2.0 OR GPL-2.0-or-later, matching teensyp.
