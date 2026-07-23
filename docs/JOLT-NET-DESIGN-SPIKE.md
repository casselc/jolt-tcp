# `jolt.net` design spike

Status: design spike, not an API commitment
Evidence baseline: installed `joltc` v0.4.15 and vendored Jolt
`d5aaf503fc7a45c5638d21215eb153b426a7e8dc`

## Conclusion

Upstream should not copy `teensyp.ffi-net` into a new namespace. Jolt already
has three partial socket stacks:

1. `jolt.mvn-http` has DNS through `getaddrinfo`, IPv4/IPv6 address iteration,
   blocking timeouts, Windows/Winsock setup, and a verified OpenSSL client.
2. `jolt.nrepl` has a small cross-platform listening path, conditional POSIX /
   Winsock bindings, synchronous startup failure, and idempotent stop.
3. `jolt-tcp` has the stronger server semantics: non-blocking sockets, captured
   `errno`, wakeable readiness, SIGPIPE protection, half-close handling,
   connection-generation ownership, and reactor cleanup.

`jolt.net` should factor the shared substrate underneath all three. Its first
public contract should be endpoints, resolution, owned sockets, byte I/O,
structured errors, and a wakeable readiness interface. Blocking clients and
reactors should be adapters over the same socket operations. TLS should layer
over the byte-I/O contract in an optional namespace, not be built into a TCP
descriptor.

The work is worthwhile even before a public reactor API exists. Moving DNS,
socket creation, error capture, addresses, lifecycle, and byte I/O upstream
eliminates the most dangerous duplicated FFI code while allowing `jolt-tcp` to
keep its existing teensyp-compatible scheduling and backpressure policy.

## Evidence inventory

### Existing code worth extracting

| Source | Reusable behavior | What must change before it is shared |
|---|---|---|
| `refs/jolt/stdlib/jolt/mvn_http.clj:51-86` | Lazy Winsock initialization and socket/DNS bindings | Separate transport initialization from OpenSSL initialization; bind platform-only symbols only on the platform where they exist |
| `refs/jolt/stdlib/jolt/mvn_http.clj:98-154` | Receive/send timeouts, `getaddrinfo`, iteration across returned families and addresses, and `freeaddrinfo` cleanup | Replace hard-coded `addrinfo` offsets with target-specific layout data; retain resolver error codes; expose the selected address; add connect deadlines/cancellation |
| `refs/jolt/stdlib/jolt/mvn_http.clj:182-325` | Verified OpenSSL client, SNI, hostname verification, TLS 1.2 minimum, memory-BIO layering, and explicit ownership cleanup | Make TLS consume a generic byte channel; preserve `WANT_READ`/`WANT_WRITE` rather than hiding readiness inside blocking loops; move native-library policy out of core TCP |
| `refs/jolt/jolt-core/jolt/nrepl.clj:24-106` | Conditional Winsock/POSIX bindings, `WSAStartup`, synchronous bind/listen failure | Replace fixed IPv4 loopback `sockaddr_in`; capture native errors; retain endpoint information |
| `refs/jolt/jolt-core/jolt/nrepl.clj:315-360` | Start binds synchronously and stop is compare-and-set idempotent | Retain and await the accept future; close accepted sockets exactly once; use shared listener lifecycle |
| `src/teensyp/ffi_net.clj:27-75` | Non-blocking POSIX bindings, `errno`, readiness constants | Move magic constants and signatures behind a platform table; add Windows and IPv6 |
| `src/teensyp/ffi_net.clj:102-150` | Non-blocking listen/accept/recv/send and explicit `:eagain`/`:eof` states | Preserve the native error at the call boundary instead of collapsing failures to `:error`; accept generic endpoints |
| `src/teensyp/ffi_net.clj:152-189` | `poll(2)` representation and self-pipe wakeup | Generalize into an owned poller and add a Winsock-compatible wake source |
| `src/teensyp/server.clj:291-437` | EOF is not close, reactor-owned final close, fd-generation checks, and callback separation | These are reactor/policy semantics, not raw socket APIs; keep them in jolt-tcp while moving their prerequisites upstream |
| `src/teensyp/server.clj:489-733` | Exactly-once cleanup, retained completion, bounded wait, and partial-start rollback | Reuse the lifecycle pattern for all upstream listeners, pollers, sockets, and TLS handles |
| `src/teensyp/stream.clj:20-105` | A native input/output connection abstraction and correct channel closure on terminal EOF | Replace the project-specific protocol with common byte input/output protocols |
| `../jolt-http/src/jolt/http/body.clj:188-312` | Blocking output `Sink` and channel-backed input `RequestBody` | Converge on shared byte source/sink protocols; keep HTTP framing wrappers in jolt-http |

### Important behavior that is currently missing

- Generic bind/connect endpoints. Both `jolt.nrepl` and `jolt-tcp` construct a
  16-byte `sockaddr_in` for `127.0.0.1`; neither can bind a hostname, wildcard,
  IPv6 address, or Unix-domain endpoint.
- Local and peer address inspection. `jolt-tcp` currently returns
  `{:local-address nil :remote-address nil :fd fd}` from
  `src/teensyp/server.clj:440-459`, forcing jolt-http to report configured
  constants instead of the actual endpoints.
- One Windows-capable readiness implementation. POSIX self-pipes cannot be
  inserted into `WSAPoll`; Windows needs a socket-based wake pair or a different
  readiness backend.
- Structured, immediate native-error capture. `jolt-tcp` reads `errno`, but
  reduces many failures to `:error`. `jolt.mvn-http` often throws with no native
  code. `jolt.nrepl` usually omits it entirely.
- A deadline/cancellation model. `SO_RCVTIMEO`/`SO_SNDTIMEO` in
  `jolt.mvn-http` bound individual blocking operations, not DNS resolution, the
  complete connect attempt across addresses, TLS handshake, or an entire
  request.
- Shared byte input and output protocols. Jolt cannot subclass Java streams, so
  `teensyp.stream`, jolt-http request bodies, and jolt-http sinks all invented
  adjacent contracts.
- Target/ABI facts sufficient to select socket constants and struct layouts.
  `addrinfo`, `sockaddr_in6`, `pollfd`, `timeval`, Windows `SOCKET`, and calling
  signatures cannot safely be inferred from OS name alone.
- Offset/length bulk byte transfer between Jolt byte arrays and native memory.
  All current stacks allocate and copy more than necessary.
- A reduced concurrent-FFI stress case. Networking cannot promise safe
  concurrency until the independently observed Chez/FFI crash is fixed and
  gated upstream.

## Proposed public model

Names below show the contract, not final spelling.

### Endpoints and resolved addresses

Use data, not a Java-shaped class hierarchy:

```clojure
(net/endpoint "example.org" 443)
;; => {:host "example.org" :port 443 :family :unspecified}

(net/endpoint "::1" 7888)
;; => {:host "::1" :port 7888 :family :inet6}

(net/resolve (net/endpoint "example.org" 443)
             {:socket-type :stream})
;; => vector of resolved-address values in resolver order
```

A resolved address must carry:

- family (`:inet`, `:inet6`, with room for `:unix` later);
- socket type and protocol;
- numeric host, port, and IPv6 scope id for inspection;
- opaque native address bytes plus length for `bind`/`connect`.

Callers must not read `addrinfo` pointers after `freeaddrinfo`. Resolution copies
each address into an owned Jolt value before freeing the native list.

For listening, `nil` host means wildcard and maps to `AI_PASSIVE`. `"localhost"`
is a name and follows resolver policy; it must not be silently rewritten to
IPv4 loopback. Address ordering should initially follow `getaddrinfo`; Happy
Eyeballs can be a later connector policy.

### Owned handles

Constructors return opaque records or maps with idempotent `:close` functions so
they work with Jolt's `with-open` behavior:

```clojure
(with-open [listener (net/listen (net/endpoint nil 8080) opts)]
  (with-open [socket (net/accept listener opts)]
    ...))
```

The public handle should expose operations, not invite raw-fd ownership:

```clojure
(net/local-endpoint socket)
(net/peer-endpoint socket)
(net/shutdown! socket :write)
(net/close! socket)
```

A diagnostic `:native-handle` may exist, but using it transfers no ownership.
Close must be compare-and-set guarded. A listener owns only its descriptor; a
server owns the accepted sockets it retains. A poller owns registrations and its
wake resources, not registered sockets. TLS owns its TLS objects but should
close the underlying byte channel only when constructed with `:close-underlying?
true`.

### Byte input/output

The minimum shared protocols should cover both directions and slices:

```clojure
(defprotocol ByteSource
  (read-bytes! [source dest off len]))

(defprotocol ByteSink
  (write-bytes! [sink src off len])
  (flush-bytes! [sink]))

(defprotocol DuplexByteChannel
  (shutdown-input! [channel])
  (shutdown-output! [channel]))
```

`read-bytes!` returns a positive count, `0` only for a zero-length request, and
`nil` for terminal EOF. Blocking adapters wait; readiness-oriented operations
may instead expose `::would-block`. A zero native `recv` means input EOF, not
full connection close. Output remains usable until explicitly shut down or an
error occurs.

`jolt-http` can implement its `RequestBody` convenience methods and chunked /
limited sinks over these protocols. `teensyp.stream` can become a blocking
adapter over them. HTTP framing, channel buffer depth, and teensyp write credit
remain application policy.

The upstream byte work should land first:

```clojure
(ffi/read-array! ptr len dest off)
(ffi/write-array ptr src off len)
(System/arraycopy src src-off dest dest-off len)
```

Without slices, a shared network API merely centralizes the existing extra
allocation and per-byte loops.

## Native operations and error contract

Every failing socket call must capture the platform error before making any
other native call:

- POSIX: read thread-local `errno` immediately;
- Windows: call `WSAGetLastError` immediately;
- resolver: retain the `getaddrinfo` return code and format it with
  `gai_strerror` where available;
- TLS: retain both `SSL_get_error` and the OpenSSL error queue where applicable.

Expected state is not exceptional:

- readiness operations return `::would-block` for `EAGAIN`/`EWOULDBLOCK`;
- interrupted waits retry or return `::interrupted` according to the supplied
  cancellation token;
- `recv == 0` returns EOF.

Other failures throw `ExceptionInfo` with stable data:

```clojure
{:jolt.net/op       :connect
 :jolt.net/kind     :connection-refused
 :jolt.net/code     111
 :jolt.net/platform :posix
 :jolt.net/endpoint {:host "127.0.0.1" :port 9}
 :jolt.net/message  "Connection refused"}
```

The stable `:kind` set should be deliberately small: `:would-block`,
`:interrupted`, `:timed-out`, `:cancelled`, `:connection-refused`,
`:connection-reset`, `:address-in-use`, `:unreachable`, `:name-resolution`,
`:invalid`, and `:unknown`. Preserve the native code even when it has no known
kind. Do not read `errno` later in a logger or exception constructor; another
FFI call may already have replaced it.

## Readiness, timeouts, and cancellation

Expose a small poller rather than making jolt-tcp's reactor the upstream API:

```clojure
(net/open-poller)
(net/register! poller socket #{:read} token)
(net/update! poller token #{:read :write})
(net/remove! poller token)
(net/wake! poller)
(net/await-ready poller deadline)
```

`await-ready` returns tokens plus event sets. Registration tokens, not raw fds,
are identities; the caller can attach a generation and reject stale work after
descriptor reuse. `:error` and `:hangup` are reported independently of read and
write interest.

Backends:

- Linux/macOS first: `poll(2)` plus the proven non-blocking self-pipe in
  `src/teensyp/ffi_net.clj:152-189`.
- Windows: `WSAPoll` plus a connected loopback UDP wake pair (both ends are
  sockets and therefore pollable). A Windows event object cannot be inserted
  into `WSAPoll`.
- Keep a finite maximum native wait as a lost-wake safety net, but compute it
  from the caller's monotonic deadline.

Timeouts should be expressed as an absolute monotonic deadline internally.
Relative `:timeout-ms` is converted once at the API boundary. A cancellation
token has a cancelled flag plus a poller wake function, so cancellation is not
delayed until an arbitrary timeout. `SO_RCVTIMEO` and `SO_SNDTIMEO` remain useful
options for simple blocking sockets but are not the system-wide cancellation
model.

DNS cancellation is constrained by blocking `getaddrinfo`; stage one documents
that limitation and applies the deadline before and after it. True cancellable
DNS requires a platform async resolver or a bounded resolver worker, which
should not be smuggled into the first patch.

## Cross-platform socket requirements

### DNS and IPv4/IPv6

- Use `AF_UNSPEC` by default and preserve resolver order.
- Copy both `sockaddr_in` and `sockaddr_in6` results without callers knowing
  their layout.
- Add `getsockname`, `getpeername`, and `getnameinfo` (or `inet_ntop`) so every
  accepted/connected socket reports real local and peer endpoints.
- Make `IPV6_V6ONLY` behavior explicit. Recommended default: follow the OS
  default in the low-level API; a higher-level server may request dual-stack or
  separate IPv4/IPv6 listeners.
- Do not add Unix-domain sockets until TCP and IPv6 semantics are stable, but
  keep the endpoint representation extensible.

### Windows/Winsock and POSIX

The binding layer selects correct signatures and close/error functions:

- Winsock `SOCKET`, `closesocket`, `ioctlsocket`, `WSAGetLastError`,
  `WSAStartup`, `WSACleanup`, and `WSAPoll`;
- POSIX fd, `close`, `fcntl`, `errno`, `poll`, and `pipe`;
- per-platform `recv`/`send` argument and return widths;
- ref-counted or once-only socket subsystem initialization.

The current nREPL conditional binding pattern
(`refs/jolt/jolt-core/jolt/nrepl.clj:51-67`) is safer than binding both
platform-only close symbols unconditionally. `WSAStartup` ownership must be
process-scoped; individual sockets must not pair it with `WSACleanup`.

### SIGPIPE

Sending to a closed peer must never terminate the Jolt process:

- Linux: `MSG_NOSIGNAL` on every `send`;
- macOS/BSD where available: `SO_NOSIGPIPE` on created and accepted sockets;
- Windows: no SIGPIPE action.

Do not globally ignore SIGPIPE as the library default because that mutates
process-wide policy. The current jolt-tcp behavior is at
`src/teensyp/ffi_net.clj:117-150`.

### Half-close and EOF

`shutdown(socket, :write)` means no more bytes will be sent; it does not close
the read side. A zero-length `recv` marks peer-write EOF only. Pollers must stop
requesting read readiness after EOF because a half-closed socket stays readable.
The application decides when its response is complete and then closes.

These are API semantics, not merely jolt-tcp implementation details. The
regressions and correct ordering are encoded in
`src/teensyp/server.clj:307-392` and
`../jolt-http/src/jolt/http/protocol.clj:935-947`.

## TLS layering

Put OpenSSL integration in an optional `jolt.net.tls.openssl` namespace:

```clojure
(tls/client socket {:server-name "example.org"
                    :verify-peer? true
                    :min-version :tls1.2})
```

The result implements the same byte source/sink/channel protocols as a plain
socket. The memory-BIO approach in `jolt.mvn-http` is a good starting point
because OpenSSL never owns the raw fd and can eventually run over non-blocking
channels. Preserve:

- default trust paths and peer verification;
- hostname verification and SNI;
- an explicit minimum TLS version;
- `WANT_READ`/`WANT_WRITE`;
- exactly-once ownership for `SSL_CTX`, `SSL`, BIOs, and the underlying socket.

Native OpenSSL discovery is policy, not TCP. Core `jolt.net` must load without
OpenSSL installed. `jolt.mvn-http` can keep its platform candidate lists until
Jolt has a general native-artifact mechanism.

Server TLS, ALPN, client certificates, and custom trust stores are follow-ups.
The first migration target is behavior-equivalent dependency download.

## Lifecycle rules

Every resource-producing operation follows the same rules:

1. Acquire incrementally and roll back all earlier acquisitions if a later step
   fails.
2. Transfer ownership only when the constructor successfully returns.
3. Put normal cleanup in an outermost `finally`.
4. Guard close with compare-and-set and publish one retained completion.
5. Make stop/close idempotent.
6. Bound public waits and report timeout structurally; cleanup continues.
7. Never let a stale worker close a raw handle owned by the readiness loop.

The local implementation at `src/teensyp/server.clj:489-732` now demonstrates
these rules for listener fd, self-pipe fds, receive/send native buffers,
connections, executors, reactor future, and completion.

Executor ownership is intentionally not part of `jolt.net`. The raw network
layer should not create handler pools. jolt-tcp currently transfers ownership of
both supplied executors and shuts them down; that legacy contract needs a
separate maintainer decision before it changes.

## Staged extraction and migration

Each stage should be independently releasable.

### Stage 0: characterization

- Pin tests to the Jolt SHA above and current main.
- Extract standalone tests for resolver iteration, Windows initialization,
  SIGPIPE, half-close, poll wakeup, timeout, and repeated start/stop.
- Add the minimal concurrent-FFI stress reproducer with OS, architecture,
  concurrency level, exact native call, and exact Jolt SHA.

Exit: every behavior that will move has a test in its current home.

### Stage 1: platform and error substrate

- Add target-driven socket constants/signatures.
- Add immediate POSIX/WSA/resolver error capture and stable `ExceptionInfo`.
- Add idempotent owned socket handles.
- Migrate `jolt.nrepl` and `jolt.mvn-http` close/error helpers without changing
  their public behavior.

Exit: no networking caller binds its own `close`/`closesocket` or reads errno
late.

### Stage 2: endpoints, DNS, addresses, and blocking byte I/O

- Extract `getaddrinfo` iteration from `jolt.mvn-http`.
- Add generic endpoints, IPv4/IPv6, bind/connect/listen/accept, local/peer
  endpoint inspection, shutdown, and byte slices.
- Migrate `jolt.mvn-http` TCP connect and `jolt.nrepl` listener.

Exit: dependency download still verifies TLS; nREPL still binds synchronously
and stops idempotently; both work on IPv4 and IPv6 where the host supports it.

### Stage 3: POSIX wakeable poller

- Extract `poll` plus self-pipe behind the poller contract.
- Preserve `::would-block`, EOF, error/hangup, generation tokens, and finite
  defensive waits.
- Replace `teensyp.ffi-net` internals while keeping `teensyp.server` and all
  teensyp-facing APIs unchanged.

Exit: the complete jolt-tcp acceptance/property suite passes, including
half-close, callback ordering, reactor error isolation, fd reuse, and immediate
same-port restarts.

### Stage 4: Windows readiness

- Add Winsock non-blocking mode, `WSAPoll`, socket wake pair, Windows error
  mapping, and Windows SIGPIPE no-op.
- Run the same reactor contract suite on Windows rather than creating a reduced
  Windows-only test set.

Exit: jolt-tcp has no platform branches outside endpoint/poller construction.

### Stage 5: TLS byte-channel adapter

- Move memory-BIO OpenSSL code into `jolt.net.tls.openssl`.
- Make handshake/read/write drive explicit readiness and deadlines.
- Migrate `jolt.mvn-http` to the adapter.

Exit: certificate, hostname, SNI, redirect, truncation, timeout, and cleanup
tests remain green; core `jolt.net` loads without OpenSSL.

### Stage 6: common stream adapters

- Land shared byte source, sink, and duplex protocols.
- Adapt `teensyp.stream` and jolt-http `RequestBody`/`Sink`.
- Keep HTTP chunking, declared-length enforcement, and parser backpressure in
  jolt-http.

Exit: projects no longer need parallel input/output protocol definitions, and
no adapter forces the non-blocking reactor into a blocking implementation.

## Test matrix

The upstream gate should cover:

- Linux, macOS, and Windows;
- IPv4 loopback, IPv6 loopback, wildcard bind, DNS with multiple answers, and
  failed resolution;
- local/peer endpoint round trips;
- connection refused, address in use, reset, would-block, timeout, and
  cancellation with stable error data;
- send after peer close without process SIGPIPE;
- peer half-close followed by a server response and clean EOF;
- poller wake while idle, wake during registration change, and bounded lost-wake
  fallback;
- fd/handle reuse with stale generation tokens;
- partial failure at every allocation/descriptor acquisition step;
- repeated close and repeated immediate bind/start/stop;
- TLS verification, hostname mismatch, truncated TLS input, and cleanup at every
  failed handshake stage;
- concurrent blocking and non-blocking FFI at and above the known failure
  threshold.

## Maintainer decisions

These choices should be explicit before the corresponding stage:

1. **Location:** put the dependency-free socket core in `jolt.net`; keep OpenSSL
   in an optional stdlib namespace. Recommended.
2. **Error surface:** tagged values for `would-block`/EOF, structured exceptions
   for failures. Recommended over returning undifferentiated `:error`.
3. **Handle representation:** opaque record/map with idempotent close versus raw
   integer handles. Recommended: opaque ownership plus diagnostic raw access.
4. **Windows wake backend:** connected UDP wake pair with `WSAPoll` versus a
   separate event-driven backend. Recommended for the first portable poller:
   socket wake pair.
5. **IPv6 listener default:** OS default versus forced dual stack. Recommended:
   expose the choice and leave the low-level default unchanged.
6. **Cancellation scope:** document blocking DNS as non-cancellable in stage
   two versus introducing resolver workers immediately. Recommended: document
   first; do not entangle executor policy with the socket substrate.
7. **TLS native acquisition:** retain `jolt.mvn-http` candidate lists temporarily
   versus blocking on a general native-artifact feature. Recommended: retain
   temporarily.
8. **jolt-tcp executor ownership:** keep transfer semantics or add explicit
   owned/borrowed options. This is a jolt-tcp API decision, not a blocker for
   `jolt.net`.
