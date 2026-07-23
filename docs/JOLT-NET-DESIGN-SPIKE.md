# `jolt.net` design spike

Status: design spike, not an API commitment
Evidence baseline (revalidated 2026-07-23):

- upstream Jolt v0.4.15 at
  `260a392a795089de3fb5ab700b386a334f01c051`;
- jolt-tcp at `db609d7e3d041c0940c4fda35049cf1ec35d0810`;
- jolt-http at `0a85401868337ac27f86d99afadbc847bef49dd3`.

The upstream recommendations 1-5 branch was still uncommitted during this
revalidation. Its proposed `jolt.host/target`, byte-array slice transfers, and
concurrent-FFI characterization are prerequisites below, not capabilities this
document assumes have shipped.

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

The source revalidation also found four ordering constraints that are easy to
miss:

1. Jolt's current `System/nanoTime` is epoch time rounded to milliseconds, not a
   monotonic clock. Deadline-bearing APIs must not ship until that host shim is
   corrected and characterized.
2. The non-blocking byte contract and endpoint/DNS layer must exist before the
   TLS adapter; otherwise OpenSSL readiness is forced back into private blocking
   loops.
3. A Windows `SOCKET` is a pointer-width unsigned handle, not a POSIX `int`.
   Treating it as `:int` makes both normal handles and `INVALID_SOCKET` unsafe on
   Win64.
4. A writable event completes a non-blocking connect attempt only after
   `getsockopt(SO_ERROR)` reports success. Readiness alone is not success.

## Evidence inventory

### Existing code worth extracting

| Source | Reusable behavior | What must change before it is shared |
|---|---|---|
| `jolt-upstream:stdlib/jolt/mvn_http.clj` (`init-sockets!`, `connect`) | Winsock initialization, receive/send timeouts, `getaddrinfo`, and iteration across IPv4/IPv6 results | Separate transport initialization from OpenSSL state; use target layouts and handle widths; retain resolver errors; add connect deadlines and actual selected endpoints |
| `jolt-upstream:stdlib/jolt/mvn_http.clj` (`tls-connect`, `tls-read`, `tls-write`) | Verified OpenSSL client, SNI, hostname verification, TLS 1.2 minimum, memory BIOs, and explicit ownership cleanup | Consume a generic readiness-aware byte channel; distinguish `WANT_READ`, `WANT_WRITE`, `close_notify`, and truncation; move native-library policy out of core TCP |
| `jolt-upstream:jolt-core/jolt/nrepl.clj` (`ensure-winsock!`, `listen-socket`, `start-server`) | Conditional Winsock/POSIX bindings, synchronous bind/listen failure, and compare-and-set stop | Replace fixed IPv4 loopback; make Winsock initialization process-scoped; retain/await workers and accepted sockets; use shared listener lifecycle |
| `jolt-tcp:src/teensyp/ffi_net.clj` | Non-blocking POSIX sockets, readiness constants, self-pipe wakeup, and Linux/macOS SIGPIPE guards | Capture errors before cleanup calls; validate all configuration calls; replace target-specific struct assumptions; add Windows and IPv6 |
| `jolt-tcp:src/teensyp/server.clj` | EOF is not close, reactor-owned final close, generation checks, retained completion, rollback, and borrowed-by-default executors | Keep scheduling/backpressure policy in jolt-tcp; reuse the resource lifecycle and owner-independent wake/close gate underneath upstream handles |
| `jolt-tcp:src/teensyp/stream.clj` | A native input/output connection abstraction and correct channel closure on terminal EOF | Replace the project-specific protocol with common blocking adapters over readiness-oriented byte operations |
| `jolt-http:src/jolt/http/body.clj` and `protocol.clj` | Blocking output `Sink`, channel-backed `RequestBody`, and tested pipelined half-close ordering | Converge on shared byte adapters; keep HTTP framing, parser backpressure, and peer-EOF notification policy in jolt-http |

### Important behavior that is currently missing

- Generic bind/connect endpoints. Both `jolt.nrepl` and `jolt-tcp` construct a
  16-byte `sockaddr_in` for `127.0.0.1`; neither can bind a hostname, wildcard,
  IPv6 address, or Unix-domain endpoint.
- Local and peer address inspection. `jolt-tcp` currently returns
  `{:local-address nil :remote-address nil :fd fd}` from
  `jolt-tcp:src/teensyp/server.clj`, forcing jolt-http to report configured
  constants instead of the actual endpoints.
- One Windows-capable readiness implementation. POSIX self-pipes cannot be
  inserted into `WSAPoll`; Windows needs a socket-based wake pair or a different
  readiness backend.
- Structured, immediate native-error capture. `jolt-tcp` reads `errno`, but
  reduces many failures to `:error`, and some constructor paths call
  `close`/`free` before reading it. `jolt.mvn-http` often throws with no native
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
- A real monotonic clock. At the baseline SHA, `System/nanoTime` is implemented
  as UTC epoch milliseconds multiplied by one million. It can jump when the
  wall clock changes and cannot support the deadline contract below.
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
Eyeballs (parallel/staggered connection policy, not just address ordering) can
be a later connector layer.

Endpoint hosts are unbracketed data: `"::1"`, not `"[::1]"`. URI adapters such
as `jolt.mvn-http` own bracket parsing and rendering. They must stop splitting
an authority at the first colon, which currently makes bracketed IPv6 URLs
unrepresentable.

The first portable resolver contract should accept ASCII DNS names and numeric
IPv4/IPv6 literals. Windows `getaddrinfo` is the ANSI entry point; silently
passing Jolt UTF-8 bytes would give platform-dependent behavior for Unicode
hostnames. Until Jolt supplies one cross-platform IDNA policy or wide-string
bindings for `GetAddrInfoW`, reject non-ASCII names as `:invalid` and document
that callers must provide an ASCII IDNA form.

A listener bound to port `0` reports the kernel-selected port from
`getsockname`; it must not keep returning the requested endpoint.

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

Do not make one method mean both "perform one non-blocking operation" and "wait
until the whole slice is transferred." The minimum shared layer has
readiness-oriented operations:

```clojure
(defprotocol ReadyByteSource
  (try-read-bytes! [source dest off len]))

(defprotocol ReadyByteSink
  (try-write-bytes! [sink src off len]))

(defprotocol DuplexByteChannel
  (shutdown-input! [channel])
  (shutdown-output! [channel]))
```

`try-read-bytes!` returns a positive count, `0` only for a zero-length request,
`::would-block`, or `::eof`. `try-write-bytes!` may make a partial write and
returns a positive count, `0` only for a zero-length request, or
`::would-block`. This makes accepted decision #2 concrete: the common core never
uses `nil` for EOF and never collapses expected state to an exception.

Blocking `ByteSource`/`ByteSink` adapters build on those methods and a
deadline/poller. A blocking read returns a positive count or `::eof`; a blocking
write transfers the whole requested slice or throws a structured timeout,
cancellation, or transport error. Legacy convenience APIs, including a
`RequestBody` method that conventionally uses `nil`, may translate `::eof` only
at their boundary.

A zero native `recv` means input EOF, not full connection close. Output remains
usable until explicitly shut down or an error occurs. A TLS implementation may
need write readiness while servicing an application read, and vice versa, so
the readiness needed for retry belongs to the TLS channel state rather than
being inferred from which public method was called.

`jolt-http` can implement its `RequestBody` convenience methods and chunked /
limited sinks over blocking adapters. `teensyp.stream` can also become a
blocking adapter. HTTP framing, channel buffer depth, and teensyp write credit
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
- TLS: clear the per-thread OpenSSL error queue before an I/O call, then call
  `SSL_get_error` on the same thread with no intervening OpenSSL call; retain
  the error queue where applicable.

`WSAStartup` is a special case: it returns its failure code directly, so that
return value is retained instead of consulting `WSAGetLastError`.

Capture precedes rollback. For example, a failed `bind` must save `errno` before
calling `close`; several current `teensyp.ffi-net` constructor paths do this in
the opposite order and can report the cleanup call's error instead.

Expected state is not exceptional:

- readiness operations return `::would-block` for `EAGAIN`/`EWOULDBLOCK`;
- a non-blocking `connect` returns `::in-progress` for
  `EINPROGRESS`/`WSAEWOULDBLOCK`; after write/error readiness the connector
  reads `SO_ERROR` and succeeds only when it is zero;
- interrupted waits retry or return `::interrupted` according to the supplied
  cancellation token;
- `recv == 0` returns `::eof`.

Other failures throw `ExceptionInfo` with stable data:

```clojure
{:jolt.net/op       :connect
 :jolt.net/kind     :connection-refused
 :jolt.net/code     111
 :jolt.net/platform :posix
 :jolt.net/endpoint {:host "127.0.0.1" :port 9}
 :jolt.net/message  "Connection refused"}
```

The stable exception `:kind` set should be deliberately small:
`:interrupted`, `:timed-out`, `:cancelled`, `:connection-refused`,
`:connection-reset`, `:address-in-use`, `:unreachable`, `:name-resolution`,
`:tls-truncated`, `:invalid`, and `:unknown`. `::would-block`, `::in-progress`,
and `::eof` are tagged expected values, not exception kinds. Preserve the native
code even when it has no known kind. Do not read `errno` later in a logger or
exception constructor; another FFI call may already have replaced it.

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
write interest. `:hangup` does not discard `:read`: a peer can close after
sending bytes, and a readiness consumer must drain those bytes before
`try-read-bytes!` returns `::eof`.

Backends:

- Linux/macOS first: `poll(2)` plus the proven non-blocking self-pipe in
  `jolt-tcp:src/teensyp/ffi_net.clj`.
- Windows: `WSAPoll` plus a connected loopback UDP wake pair (both ends are
  sockets and therefore pollable). A Windows event object cannot be inserted
  into `WSAPoll`.
- Keep a finite maximum native wait as a lost-wake safety net, but compute it
  from the caller's monotonic deadline.

The mutation queue/registration state, not the number of wake bytes or
datagrams, is the source of truth. Wake writes may coalesce; draining continues
until `::would-block`, and a wake send that would block means a wake is already
queued. Serialize wake writes against closing the wake handle with an
owner-independent atomic gate. Do not use `ReentrantLock` for this gate: Jolt
futures can share host thread-identity bookkeeping, and jolt-tcp has a concrete
failure where two futures were treated as one reentrant owner. A finite wait
still bounds damage from a lost token.

Timeouts should be expressed as an absolute monotonic deadline internally, but
this is blocked on a real monotonic source. At the baseline SHA,
`System/nanoTime` calls UTC `current-time`, rounds it to milliseconds, and
multiplies by one million. Stage 0 must replace or bypass that implementation
with a host monotonic clock (for example the platform's monotonic/performance
counter) and prove non-decreasing behavior independently from wall-clock
changes. Only then should relative `:timeout-ms` be converted once at the API
boundary.

A cancellation token has a cancelled flag plus a poller wake function, so
cancellation is not delayed until an arbitrary timeout. `SO_RCVTIMEO` and
`SO_SNDTIMEO` remain useful options for simple blocking sockets but are not the
system-wide cancellation model.

DNS cancellation is constrained by blocking `getaddrinfo`; Stage 2 documents
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
- Preserve IPv6 scope ids and use numeric `getnameinfo` flags for inspection;
  endpoint reporting must never trigger reverse DNS.
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
(`jolt-upstream:jolt-core/jolt/nrepl.clj`) is safer than binding both
platform-only close symbols unconditionally. `WSAStartup` ownership must be
process-scoped and once-only; individual sockets must not pair it with
`WSACleanup`. Initialization failure must also be memoized or retried by an
explicit subsystem policy, not accidentally repeated every time OpenSSL loading
fails as it is in `jolt.mvn-http`.

On Win64, bind `SOCKET` arguments and results as `:uptr` and compare creation /
accept results to the all-bits-one `INVALID_SOCKET` value. A valid Windows
socket may use any other unsigned handle value, so `neg?` and a `:int` return
type are invalid tests. `WSAPOLLFD` is a target layout containing pointer-width
`SOCKET` plus two `SHORT` fields; it is not the eight-byte POSIX `pollfd`.
Likewise, `ioctlsocket(FIONBIO)` receives a pointer to `u_long`, not the POSIX
`fcntl` shape.

Treat `WSAPoll` result flags as data, not as a POSIX compatibility guarantee.
In particular, failed non-blocking TCP connects can report hangup/error/write
flags together on current Windows. The portable connector still resolves the
outcome through `SO_ERROR`.

### SIGPIPE

Sending to a closed peer must never terminate the Jolt process:

- Linux: `MSG_NOSIGNAL` on every `send`;
- macOS/BSD where available: `SO_NOSIGPIPE` on created and accepted sockets;
- Windows: no SIGPIPE action.

Do not globally ignore SIGPIPE as the library default because that mutates
process-wide policy. The current jolt-tcp behavior is in
`jolt-tcp:src/teensyp/ffi_net.clj`.

### Half-close and EOF

`shutdown(socket, :write)` means no more bytes will be sent; it does not close
the read side. A zero-length `recv` marks peer-write EOF only. Pollers must stop
requesting read readiness after EOF because a half-closed socket stays readable.
The application decides when its response is complete and then closes.

These are API semantics, not merely jolt-tcp implementation details. The
regressions and correct ordering are encoded in
`jolt-tcp:src/teensyp/server.clj` and
`jolt-http:src/jolt/http/protocol.clj`.

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
- DNS-name verification and SNI;
- IP-address verification through the OpenSSL IP-reference API, without sending
  an IP literal as SNI;
- an explicit minimum TLS version;
- `WANT_READ`/`WANT_WRITE` as retry-interest transitions (either can arise from
  an application read or write);
- `SSL_ERROR_ZERO_RETURN` as clean peer `close_notify`, distinct from transport
  EOF without `close_notify`, which is a truncated-TLS error by default;
- exactly-once ownership for `SSL_CTX`, `SSL`, BIOs, and the underlying socket.

The existing `jolt.mvn-http` loop treats `WANT_READ` and `WANT_WRITE` alike and
collapses several fatal/truncated read paths to `nil`. The adapter must instead
flush pending output on write interest, feed ciphertext on read interest, retry
the same TLS operation, and preserve the OpenSSL error before any other OpenSSL
call. TLS `close_notify` closes only the peer's TLS write direction; buffered
application bytes are delivered first and local output may remain usable until
the application shuts it down.

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
8. Retain every accept/reactor/connection worker that cleanup must await.
   Re-check ownership after a blocking `accept`; if stop won the race, close the
   just-accepted socket rather than dropping its handle.
9. Use owner-independent atomics for short ownership gates. Do not assume Jolt
   future identity maps one-to-one to a native thread-lock owner.

The local implementation at
`jolt-tcp:src/teensyp/server.clj` now demonstrates these rules for listener fd,
self-pipe fds, receive/send native buffers, connections, executors, reactor
future, and completion. That does not make the current raw
`teensyp.ffi-net` constructors the extraction target unchanged: their late
`errno` reads and unchecked configuration calls still need the stage-one error
discipline.

Executor ownership is intentionally not part of `jolt.net`. The raw network
layer should not create handler pools. Ownership is a jolt-tcp API decision.
The accepted policy is now implemented: jolt-tcp always shuts down pools it
creates, borrows supplied pools by default, and adopts a supplied pool only when
the caller sets `:shutdown-executor?` or
`:shutdown-callback-executor?`. The same rule governs startup rollback and
normal stop, so a borrowed application pool is never silently shut down.

## Prototype sequencing

This checkpoint deliberately stops at a source-grounded design rather than
adding a `jolt.net` prototype to jolt-tcp:

- putting the namespace in jolt-tcp would shadow, rather than validate, its
  intended upstream home;
- the target facts, checked byte slices, and concurrent-FFI gate are being
  developed by upstream recommendations 1-5 and were not yet a committed
  baseline during this spike;
- a deadline-aware resolver/connector/poller prototype would encode a false
  guarantee while `System/nanoTime` is wall-clock based;
- only Linux can be executed in the current local environment, while the
  accepted design deliberately includes Win64 handle/layout and macOS SIGPIPE
  behavior.

The first code spike should therefore start from a clean upstream worktree after
recommendations 1-5 land and the monotonic clock is corrected. Its narrow scope
is Stage 1 plus the Stage 2 endpoint/resolver value: target-selected layouts,
copied `getaddrinfo` results, numeric endpoint inspection, immediate structured
errors, and IPv4/IPv6 loopback tests. It should not yet include a reactor, TLS,
or a jolt-tcp migration.

## Staged extraction and migration

Each stage should be independently releasable.

### Stage 0: characterization

- Pin tests to the Jolt SHA above and current main.
- Extract standalone tests for resolver iteration, Windows initialization,
  SIGPIPE, half-close, poll wakeup, timeout, and repeated start/stop.
- Add the minimal concurrent-FFI stress reproducer with OS, architecture,
  concurrency level, exact native call, and exact Jolt SHA.
- Characterize `System/nanoTime`: non-decreasing under concurrency, finer than
  wall-clock milliseconds where supported, and unaffected by wall-clock
  adjustment. This test is expected to fail at the baseline SHA.

Exit: every behavior that will move has a test in its current home.

### Stage 1: platform and error substrate

- Add target-driven socket constants/signatures.
- Land checked offset/length byte-array transfers.
- Provide a real monotonic host clock and make `System/nanoTime` use it.
- Add immediate POSIX/WSA/resolver error capture and stable `ExceptionInfo`.
- Add idempotent owned socket handles.
- Gate networking on the reduced concurrent-FFI stress case.
- Migrate `jolt.nrepl` and `jolt.mvn-http` close/error helpers without changing
  their public behavior.

The active upstream recommendations 1-5 work is intended to supply several of
these prerequisites, but this plan treats them as complete only after a clean
commit and its advertised test matrix.

Exit: no networking caller binds its own `close`/`closesocket` or reads errno
late; target facts and slice transfers are public and tested; deadline math has
a monotonic source.

### Stage 2: endpoints, DNS, addresses, and byte channels

- Extract `getaddrinfo` iteration from `jolt.mvn-http`.
- Add generic endpoints, IPv4/IPv6, bind/connect/listen/accept, local/peer
  endpoint inspection, shutdown, and actual port-0 reporting.
- Land the readiness-oriented byte protocols and `::would-block` / `::eof`
  contract, plus blocking adapters sufficient for the current consumers.
- Scope the first DNS contract to ASCII/IDNA-form names; add bracketed-IPv6 URI
  tests to the `jolt.mvn-http` adapter.
- Migrate `jolt.mvn-http` TCP connect and `jolt.nrepl` listener.

Exit: dependency download still verifies TLS; nREPL still binds synchronously
and stops idempotently; both work on IPv4 and IPv6 where the host supports it.

### Stage 3: POSIX wakeable poller

- Extract `poll` plus self-pipe behind the poller contract.
- Add deadline/cancellation wakeups and non-blocking connect completion through
  `SO_ERROR`.
- Preserve `::would-block`, `::eof`, readable-plus-hangup draining, generation
  tokens, and finite defensive waits.
- Replace `teensyp.ffi-net` internals while keeping `teensyp.server` and all
  teensyp-facing APIs unchanged.

Exit: the complete jolt-tcp acceptance/property suite passes, including
half-close, callback ordering, reactor error isolation, fd reuse, and immediate
same-port restarts.

### Stage 4: Windows readiness

- Add Winsock non-blocking mode, `WSAPoll`, socket wake pair, Windows error
  mapping, pointer-width handles/layouts, once-only `WSAStartup`, and Windows
  SIGPIPE no-op.
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

- Adapt `teensyp.stream` and jolt-http `RequestBody`/`Sink`.
- Keep HTTP chunking, declared-length enforcement, and parser backpressure in
  jolt-http.

Exit: projects no longer need parallel input/output protocol definitions, and
no adapter forces the non-blocking reactor into a blocking implementation.

## Test matrix

The upstream gate should cover:

- Linux, macOS, and Windows in ordinary CI, not only release-time smoke builds;
- IPv4 loopback, IPv6 loopback, wildcard bind, DNS with multiple answers, and
  failed resolution;
- ASCII/IDNA-form DNS input, explicit rejection of non-ASCII v1 input,
  bracketed-IPv6 URI adaptation, and numeric inspection without reverse DNS;
- local/peer endpoint round trips, IPv6 scope ids, and port-0 bind reporting;
- connection refused, address in use, reset, would-block, in-progress connect,
  `SO_ERROR` completion, timeout, and cancellation with stable error data;
- injected failures proving POSIX/WSA/resolver error capture occurs before
  cleanup calls;
- monotonic deadline behavior under concurrency and wall-clock adjustment;
- send after peer close without process SIGPIPE;
- peer half-close followed by a server response and clean EOF, including final
  readable bytes delivered together with hangup;
- poller wake while idle, wake during registration change, and bounded lost-wake
  fallback, plus wake-buffer saturation/coalescing;
- Win64 high-bit socket handles, exact `INVALID_SOCKET` comparison,
  target-selected `WSAPOLLFD`, and concurrent once-only `WSAStartup`;
- fd/handle reuse with stale generation tokens;
- partial failure at every allocation/descriptor acquisition step;
- repeated close and repeated immediate bind/start/stop;
- TLS DNS-name and IP-SAN verification, hostname mismatch, `WANT_READ` /
  `WANT_WRITE` transitions in both application directions, clean
  `close_notify`, truncated TLS input, and cleanup at every failed handshake
  stage;
- concurrent blocking and non-blocking FFI at and above the known failure
  threshold.

## Maintainer decisions

**Status: accepted (2026-07-23).** The maintainer has accepted all eight
recommendations below as the decisions of record; each takes effect before its
corresponding stage. "Recommended" is retained inline to show the reasoning that
was accepted.

1. **Location:** put the dependency-free socket core in `jolt.net`; keep OpenSSL
   in an optional stdlib namespace. Recommended. **Accepted.**
2. **Error surface:** tagged values for would-block/EOF and structured
   exceptions for failures. Recommended over returning undifferentiated
   `:error`. **Accepted.** The `try-*` protocol shape, exact qualified keyword
   spelling, and analogous in-progress-connect tag above remain design
   recommendations rather than frozen API names.
3. **Handle representation:** opaque record/map with idempotent close versus raw
   integer handles. Recommended: opaque ownership plus diagnostic raw access.
   **Accepted.**
4. **Windows wake backend:** connected UDP wake pair with `WSAPoll` versus a
   separate event-driven backend. Recommended for the first portable poller:
   socket wake pair. **Accepted.**
5. **IPv6 listener default:** OS default versus forced dual stack. Recommended:
   expose the choice and leave the low-level default unchanged. **Accepted.**
6. **Cancellation scope:** document blocking DNS as non-cancellable in stage
   two versus introducing resolver workers immediately. Recommended: document
   first; do not entangle executor policy with the socket substrate.
   **Accepted.**
7. **TLS native acquisition:** retain `jolt.mvn-http` candidate lists temporarily
   versus blocking on a general native-artifact feature. Recommended: retain
   temporarily. **Accepted.**
8. **jolt-tcp executor ownership: Accepted and implemented —
   *borrowed-by-default*.** jolt-tcp reaps only pools it created
   (`:owns-executor?`), and a caller who wants a
   supplied pool adopted opts in with `:shutdown-executor?` /
   `:shutdown-callback-executor?`. Recommended over the former
   transfer-on-success behavior for three reasons. First, least surprise: a
   supplied `ExecutorService` is commonly shared across servers or mixed with
   other application work, and silently shutting it down on `stop-server` is a
   footgun that matches no mainstream server library. Second, the failure modes
   are asymmetric: leaking a pool the caller still references is recoverable
   (the caller, or process exit, reclaims it), whereas shutting down a pool the
   caller still uses elsewhere causes unrecoverable `RejectedExecutionException`
   in unrelated code. Third, it simplifies the implementation — the shutdown
   decision collapses to explicit cleanup flags, the
   `:executors-transferred?` flag and its reset in `run-server` disappear, and
   `cleanup-server!` and
   `cleanup-startup!` become symmetric (both reap iff owned, plus the explicit
   opt-in). `with-open` ergonomics are preserved because the common no-executor
   case still self-cleans. Migration cost is one greppable, documented break:
   callers relying on jolt-tcp reaping their supplied pool add
   `:shutdown-executor? true`. This is a jolt-tcp API decision, not a blocker
   for `jolt.net`.

### Follow-up recommendation from revalidation

**Hostname input v1: recommended, not yet an API commitment.** Accept ASCII DNS
names (including caller-provided IDNA A-labels) and numeric IPv4/IPv6 literals;
reject other Unicode input structurally. Defer automatic IDNA processing and
Windows `GetAddrInfoW` until Jolt can define one normalization contract and
supply portable wide-string support. This is preferable to silently sending
UTF-8 bytes through Windows' ANSI `getaddrinfo` while POSIX platforms behave
differently.
