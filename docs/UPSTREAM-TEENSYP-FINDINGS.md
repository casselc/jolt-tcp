# Findings for upstream teensyp

Behaviour in **[weavejester/teensyp](https://github.com/weavejester/teensyp)**
that jolt-tcp had to change, work around, or deliberately not port, and that
looks like a defect or a gap in the JVM original rather than a jolt-specific
concern.

This is a report *about* upstream, distinct from
[`UPSTREAM-IMPROVEMENTS.md`](UPSTREAM-IMPROVEMENTS.md), which records what
**Jolt** (the language and runtime) would need to make jolt-tcp smaller. Nothing
here is about Chez, FFI, or AOT.

## Baseline and method

| | |
| --- | --- |
| Upstream reviewed | `weavejester/teensyp` @ `879da3519480b33cc4c0db3680337d99519ab534` (2026-07-28, "Add a note to README about Babashka support") |
| Compared against | `jolt-tcp` @ `0c3e085`, this branch |
| Method | Source review of all 754 lines of `src/clj/teensyp/`, driven by the defects the jolt-tcp port hit in its own equivalent code paths |

**Findings 3 and 7 have been reproduced on the JVM** — see the *Confirmed*
column below. The rest are derived from reading upstream source. For those, the
jolt port is a reimplementation on a different transport, so a runtime witness
there is evidence that a *class* of bug exists in the shared design, not that
the JVM code fails identically; each carries a reproduction sketch that should
be run before filing. Line references are `file:line` against the SHA above.

The confirmed pair was checked by building upstream at the SHA above (JDK 21,
Clojure 1.12.5) and differential-testing `teensyp.buffer` against an
independently written reference implementation over every string on the
alphabet `{CR, LF, x}` up to length 5, at every buffer position, for four
needles — 364 strings, ~7300 comparisons:

| | `index-of-array` mismatches | `read-line` mismatches | exceptions thrown |
| --- | ---: | ---: | ---: |
| upstream `879da351` | 2213 | 263 | 2135 |
| with the fix in finding 7 and 3 | 0 | 0 | 0 |

Severity is about impact on a server that reaches production, not about how
hard the bug is to reach.

## Summary

| # | Severity | Confirmed | Area | Finding |
| --- | --- | --- | --- | --- |
| [1](#1) | High | reasoned | reactor | An unhandled exception on the accept or pending path terminates the whole server, silently |
| [2](#2) | High | reasoned | write API | Write callbacks are success-only, so a blocking producer parks forever when a write fails |
| [3](#3) | High | **executed** | buffer | `read-line` reads the byte *before* the buffer position; a leading bare LF throws |
| [4](#4) | Medium-High | reasoned | lifecycle | A caller-supplied executor is always shut down, shutdown cannot be awaited, and queued writes are dropped |
| [5](#5) | Medium-High | reasoned | protocol | Read EOF closes the connection immediately, so half-close request/response cannot work |
| [6](#6) | Medium-High | reasoned | stream | `stream-handler` shares one close-state atom across every connection |
| [7](#7) | Medium-High | **executed** | buffer | `index-of-array` never finds a needle in the final bytes, and throws on a short buffer |
| [8](#8) | Medium | reasoned | executor | Handler calls and write callbacks share one pool, which deadlocks a blocking sink at `pool-size` |
| [9](#9) | Medium | reasoned | options | `:reuse-address?` and `:recv-buffer-size` are applied after `bind`, so both are no-ops |
| [10](#10) | Low-Medium | reasoned | queues | Queue admission is check-then-act, so a concurrent producer gets `IllegalStateException` |
| [11](#11) | Low | reasoned | misc | Callback thread is inconsistent; `run-writer`-style continuations recurse; close arity drops its exception |

---

<a id="1"></a>
## 1. An unhandled exception on the accept or pending path kills the server

**Severity: high (availability).**

`server-loop` wraps its loop in `try`/`finally` with **no `catch`**
(`server.clj:376-393`). `handle-read` and `handle-write` each catch
`IOException` (`server.clj:274-285`, `287-306`), but three paths are unguarded:

- **`handle-accept` catches nothing** (`server.clj:223-238`).
  `ServerSocketChannel.accept` is documented to return `null` on a non-blocking
  channel when no connection is pending, which makes `(.configureBlocking ch
  false)` an NPE. It also throws `IOException` for `ECONNABORTED` — a client
  that connects and immediately resets, which is ordinary background noise on a
  public listener — and for `EMFILE`/`ENFILE` under fd pressure.
- **`handle-key` re-checks `.isValid` before each capability test**
  (`server.clj:308-315`), but `handle-close` runs on a *worker* thread
  (`server.clj:234`, `261`) and closes the channel there, cancelling the key.
  A worker cancelling between the reactor's `.isValid` check and its
  `.isReadable` call yields `CancelledKeyException`. `update-flags` already
  guards for exactly this (`server.clj:54-61`), so the hazard is known; the
  dispatch path is not covered.
- **`handle-pending` → `handle-control` → `submit`** (`server.clj:342-357`)
  throws `RejectedExecutionException` if the executor is shutting down.

Any of these unwinds through `foreach!` into the `finally`, which shuts the
executor down, runs every close arity, and closes the selector. The server is
then dead, but `run-server`'s returned `Closeable` still looks healthy — the
reactor `Thread` has no uncaught-exception handler, so the only trace is a
stack dump on stderr. There is no `:error-logger` option to route it anywhere.

**Reproduction sketch.** Point a connect-and-RST loop at the listener until
`accept` returns `ECONNABORTED`; or lower `ulimit -n` below the connection
count to force `EMFILE`. Either should stop the server rather than the
connection.

**Suggested shape.** Isolate per-key dispatch in `try`/`catch Throwable`, close
the offending connection, log through a configurable `:error-logger`, and let
the reactor continue. Treat a `null` from `accept` as "nothing to do". Accept
errors that are not per-connection (`EMFILE`) should back off rather than exit.

*jolt-tcp:* `:error-logger` is a documented option, per-connection errors close
that connection only, and the reactor survives. `:accept-batch-size` bounds one
readiness turn, and an accepted socket that fails registration is closed and
rolled back (`src/teensyp/server.clj:830-869`).

<a id="2"></a>
## 2. Write callbacks are success-only, so failures strand blocking producers

**Severity: high (liveness).**

`handle-write` invokes a queued buffer's callback only after the buffer is
fully written (`server.clj:298`, `303`). On `IOException` it jumps to
`handle-close` (`server.clj:305-306`) and every callback still in the write
queue is discarded. `handle-close` from the read path (`server.clj:284-285`) drops
them the same way. `write` has no failure arity to call — the contract is one
zero-argument function meaning "success" (`server.clj:99-114`).

This is fine for fire-and-forget writes and fatal for the blocking-sink pattern
that teensyp itself ships. `socket->output-stream`'s `blocking` helper parks the
calling thread and spins until the callback sets `done`:

```clojure
;; stream.cljc:76-84
blocking (fn [f]
           (let [thread (Thread/currentThread)]
             (f #(do (vreset! done true) (LockSupport/unpark thread)))
             (while (not @done) (LockSupport/park))
             (vreset! done false)))
```

If the peer resets mid-write, the callback never runs, `done` stays false, and
the thread parks **forever**. There is no timeout and no interrupt handling. On
the default fixed pool of 32 (`stream.cljc:97-98`) that permanently retires a
worker per reset connection; enough of them and the stream layer stops serving.

A second instance: `close` queues `::close` through the write queue
(`server.clj:116-120`), but `interest-ops` clears `OP_WRITE` once `CLOSED` is
set (`server.clj:48-52`), so if the socket is already closed the `::close` entry
is never drained and its callback never fires. `socket->output-stream`'s default
`on-close` blocks on exactly that callback (`stream.cljc:85`), so **closing an
OutputStream over an already-closed socket parks forever too.**

**Reproduction sketch.** `stream-handler` echoing a large body; client sends,
then `SO_LINGER 0` reset mid-response. The stream thread should never return.

**Suggested shape.** Add a completion-bearing write alongside the existing
callback — a `CompletableFuture`/promise settled `:written` or `:failed` — and
settle every queued outcome with the failure before retiring the connection.
Keeping the zero-arg callback success-only is fine for compatibility as long as
the blocking helpers move to the new API.

*jolt-tcp:* `write-completion` returns a promise settling `{:status :written}`
or `{:status :failed :exception ex}`; the reactor settles every queued outcome
on native write error *before* retirement, and `teensyp.stream` waits on that
instead of the callback (`src/teensyp/stream.clj:42-54`). The bounded model and
its known-bug control are in
[`proofs/reactor-lifecycle-invariants.md §4`](proofs/reactor-lifecycle-invariants.md).
The retained success-only edge is recorded there as a deliberate SAT limitation
rather than hidden.

<a id="3"></a>
## 3. `buffer/read-line` reads before the buffer position

**Severity: high (remotely triggerable connection kill). Confirmed on the
JVM.** This one reaches Capra directly — I reproduced it end-to-end against a
live Capra server, where it drops the connection with no response at all.

```clojure
;; buffer.clj:59-72
(defn read-line [^ByteBuffer buffer ^Charset charset]
  (let [CR 0x0D, LF 0x0A
        start (.position buffer)
        index (index-of buffer LF)]
    (when (not= index -1)
      (let [len (if (= CR (.get buffer (dec index)))    ; <-- index may equal start
                  (- index start 1)
                  (- index start))
```

When the line is empty — the LF sits at the position itself — `(dec index)` is
`start - 1`. teensyp resets the handler's read view to position 0 before every
dispatch (`server.clj:247-249`), so `start` is routinely 0 and the probe becomes
`(.get buffer -1)`, which throws `IndexOutOfBoundsException`. The exception is
caught by `submit-read-handler` (`server.clj:251-266`), which closes the connection.

So **a client whose first byte is a bare LF gets its connection dropped**:

```
printf '\nGET / HTTP/1.1\r\nHost: x\r\n\r\n' | nc localhost 8080
```

RFC 9112 §2.2 asks a server to *ignore* at least one empty line before the
request line, so this is a case a well-behaved server is expected to tolerate.

At `start > 0` the probe reads an already-consumed byte instead of throwing. At
every current call site that byte is the previous line's LF, so it degrades
silently rather than crashing — but the guard is positional luck, not
invariant. Were it ever a CR, `len` would be `-1` and `(byte-array -1)` would
throw `NegativeArraySizeException`.

**Fix.** Require the probe to stay at or after `start`:

```clojure
(if (and (> index start) (= CR (.get buffer (dec index))))
  (- index start 1)
  (- index start))
```

*jolt-tcp:* fixed with that exact guard and a comment naming the case
(`src/teensyp/buffer.clj:141-158`), plus a generative property checking
`read-line` against an independent naive implementation.

<a id="4"></a>
## 4. Executor ownership, shutdown determinism, and lost close arities

**Severity: medium-high.** Four distinct problems in one code path
(`server.clj:376-393`, `369-374`, `441-457`).

**a. A supplied executor is always shut down.** `run-server` accepts
`:executor` and the loop's `finally` unconditionally calls `.shutdown` and
`.awaitTermination` on it. A pool shared with the rest of the application is
destroyed when the server stops. Ownership should be explicit — borrow by
default, adopt only on request.

**b. Shutdown cannot be awaited.** `close` does `.close server-ch` and
`.wakeup selector` and returns immediately; all the real cleanup happens later
on the reactor thread. A caller cannot tell when connections are retired, and a
test cannot deterministically start a second server on the same port.

**c. Queued writes are discarded.** `shutdown-key` just closes the channel
(`server.clj:369-374`). A response already handed to `write` but not yet
flushed is lost, and (per finding 2) its callback never fires. Combined with
the 60-second `awaitTermination`, a handler blocked on such a callback is
waited on for a full minute and then abandoned mid-write.

**d. The close arity is delivered with the wrong exception, and possibly
twice.** `shutdown-key` calls `(handler state nil)` — always `nil`, discarding
the `close-ex` that `handle-pending-close` would have passed
(`server.clj:327-334`). Nothing records that a close arity already ran, and a
key whose channel was closed by a worker stays in `.keys` until the next
`select` compacts it, so a connection that closed just before shutdown can get
its close arity **twice**: once with the real exception, once with `nil`.
Handlers that release resources there will double-release.

**Suggested shape.** Give `close` a bounded, awaitable, idempotent shutdown:
stop accepting, quiesce active handlers, drain writes still owed to them, run
each close arity exactly once with its recorded exception, then retire sockets
and only-owned executors.

*jolt-tcp:* `stop-server` is idempotent and bounded by `:stop-timeout-ms`,
continues draining writes for already-active handlers while refusing new work,
runs each close arity exactly once, and throws structured
`:teensyp.server/stop-timeout` on overrun. Supplied executors are borrowed
unless `:shutdown-executor?`/`:shutdown-callback-executor?` opt in.

<a id="5"></a>
## 5. Read EOF closes the connection, so half-close cannot be served

**Severity: medium-high (protocol correctness).**

```clojure
;; server.clj:278-280
(if (neg? (.read ch read-buffer))
  (handle-close key nil)   ; handle-close does (-> key .channel .close)
  ...)
```

A peer's `shutdown(SHUT_WR)` reaches the server as `read` returning -1, and
teensyp responds by closing the socket outright (`server.clj:214-221`). But a
half-closing peer has usually finished *sending* and is still *reading*, waiting
for a reply. The reply is discarded along with anything already queued.

This breaks the standard `send request → shutdown write → read response` client
shape, which is what `curl`, many HTTP clients, and most protocol test harnesses
do. Any adapter layered on teensyp inherits it.

The handler cannot compensate: it is never told the peer half-closed, only that
the connection is gone, and by then the channel is closed.

**Suggested shape.** On EOF, record peer-half-closed, stop reading, deliver a
final read arity so the handler can see end-of-request, and leave the write side
open. The handler owns the close, because only it knows whether a response is
still owed — the reactor cannot tell.

*jolt-tcp:* `peer-closed?` reports the half-close, the read arity is called once
when it becomes true (possibly with an empty buffer), and the connection stays
open until the handler closes it. The ordering constraint — that late bytes are
delivered before end-of-stream is exposed — is modelled in
[`proofs/reactor-lifecycle-invariants.md §5`](proofs/reactor-lifecycle-invariants.md),
whose control case (closing on raw `peer-closed?`) is SAT with bytes hidden.

<a id="6"></a>
## 6. `stream-handler` shares close-state across every connection

**Severity: medium-high (cross-connection state leak).**

```clojure
;; stream.cljc:188-195
([f options]
 (let [closed       (atom 0)
       on-close-out #(when (= 3 (swap! closed bit-or 1)) (tcp/close %))
       on-close-in  #(when (= 3 (swap! closed bit-or 2)) (tcp/close %))]
   (input-stream-handler ...)))
```

`closed` is created **once, when `stream-handler` is called**, and the handler
it returns is installed for the whole server. Every connection therefore
mutates the same atom. Two consequences:

- Connection A closing its output stream sets bit 1; connection B then closing
  its *input* stream sets bit 2, reaches 3, and closes **B's** socket while B's
  output stream is still open.
- Once any connection drives the atom to 3 it stays 3, so from then on every
  connection closes as soon as *either* stream closes, instead of both.

The intended semantics ("closed when both streams are closed", per the
docstring at `stream.cljc:170-187`) hold only for the first connection, and
only if it closes both streams itself.

**Fix.** Move `closed` inside the accept arity so each connection gets its own.

*jolt-tcp:* per-connection state is constructed inside the accept arity
(`src/teensyp/stream.clj:80`).

<a id="7"></a>
## 7. `index-of-array` never finds a needle in the final bytes

**Severity: medium-high (public API). Confirmed on the JVM.** I originally
recorded this as an out-of-bounds throw; testing showed a second and worse
defect underneath it.

```clojure
;; buffer.clj:43-57
(let [b   (aget needle 0)
      end (- (.limit buffer) (alength needle) 1)]   ; <-- one short
  (loop [i (.position buffer)]
    (if (and (= b (.get buffer i))                  ; <-- unguarded probe
             (matches-tail-bytes? buffer i needle))
      i
      (if (< i end) (recur (inc i)) -1))))
```

**a. The bound is one index short — a false negative.** For a needle of `m`
bytes the last legal start index is `limit - m`. `end` is `limit - m - 1`, and
because the continuation test `(< i end)` sits in the *else* branch, the last
index ever tested is `end` itself. The legal final position is never probed, so
a needle occupying the last `m` bytes is reported absent:

```clojure
(buf/index-of-array (ByteBuffer/wrap (.getBytes "xxx\r\n")) (.getBytes "\r\n"))
;;=> -1     ; confirmed on upstream 879da351; should be 3
```

This is the serious half. For an incremental framing parser — the function's
entire purpose — a false negative on a delimiter that has already arrived means
the caller waits for bytes the peer already sent. That is a hang, not an
exception, and it resolves only if more data happens to arrive and shift the
delimiter out of the tail.

**b. The probe is unguarded — a throw.** `(.get buffer i)` and
`matches-tail-bytes?` (`buffer.clj:36-41`) run before any bounds check. When
the buffer holds fewer bytes than the needle, `matches-tail-bytes?` reads
`i+1 … i+m-1` past the limit and `ByteBuffer.get(int)` throws
`IndexOutOfBoundsException`. On an **empty** buffer — `position == limit`, the
normal state while waiting for more data — the very first probe throws.

Both were confirmed by differential testing against an independent reference
implementation: 2213 wrong answers and 2135 exceptions over ~7300 cases.

**Fix.** Compute `end` as the last legal *start* index and bound the loop by it
before reading:

```clojure
(let [b   (aget needle 0)
      end (- (.limit buffer) (alength needle))]
  (loop [i (.position buffer)]
    (if (<= i end)
      (if (and (= b (.get buffer i)) (matches-tail-bytes? buffer i needle))
        i
        (recur (inc i)))
      -1)))
```

An empty or too-short region makes `end < position`, so the loop returns -1
without touching the array. This also matches the shape of `index-of` directly
above it, which already tests its bound first. With this applied, the same
differential sweep reports zero mismatches and zero exceptions.

*jolt-tcp:* fixed the same way, with the reasoning in a comment
(`src/teensyp/buffer.clj:120-139`) and a property asserting agreement with a
naive implementation over generated buffers and needles.

<a id="8"></a>
## 8. Handler calls and write callbacks share one executor

**Severity: medium (deadlock under a documented usage pattern).**

`server-loop` builds a single `submit` (`server.clj:379`) used for handler
arities (`server.clj:233`, `260`), write and control callbacks
(`server.clj:298`, `303`, `349`, `352`), and close arities (`server.clj:366`).
The default pool is `2 + availableProcessors` fixed (`server.clj:395-397`).

teensyp ships a blocking sink — `socket->output-stream` (finding 2) parks the
writing thread until the write callback runs. If that producer is running on a
handler arity, the thread it needs to release it comes from the same fixed
pool. `pool-size` concurrent connections writing large bodies occupy every
thread, each waiting for a callback that can only be scheduled once one of them
returns. The pool deadlocks at exactly `pool-size`.

`stream-handler` dodges this by accident: `input-stream-handler` runs `f` on a
*separate* default pool (`stream.cljc:97-98`, `150`), so the producer is not on
a server thread. A handler that writes and blocks directly — the obvious thing
to do — has no such separation.

**Fix.** Run completion callbacks on an executor distinct from the handler
executor, and reject configurations that pass the same object for both.

*jolt-tcp:* `:callback-executor` is a separate, configurable pool (default 2),
and passing the same executor object for both roles is rejected at startup.

<a id="9"></a>
## 9. `:reuse-address?` and `:recv-buffer-size` are applied after `bind`

**Severity: medium (silent no-op).**

```clojure
;; server.clj:15-18
(defn- server-socket-channel ^ServerSocketChannel [port]
  (doto (ServerSocketChannel/open)
    (.configureBlocking false)
    (.bind (InetSocketAddress. port))))     ; <-- bind happens here

;; server.clj:441-450
(let [server-ch (server-socket-channel port) ...]
  (when reuse-address?
    (.setOption server-ch StandardSocketOptions/SO_REUSEADDR true))   ; too late
  (when recv-buffer-size
    (.setOption server-ch StandardSocketOptions/SO_RCVBUF ...)))      ; too late
```

`SO_REUSEADDR` only affects the bind it precedes, so `:reuse-address? true`
does nothing — the restart-after-`TIME_WAIT` case it exists for still fails
with `BindException`. `SO_RCVBUF` on a listening socket must also precede
`bind`: it is the value inherited by accepted sockets, and on Linux it is what
determines whether window scaling is negotiated. Setting it afterwards leaves
accepted connections on the default.

Both options are documented as working (`server.clj:412-413`).

**Fix.** Pass the options into `server-socket-channel` and apply them between
`open` and `bind`.

Two smaller things in the same block: `(Integer. ^long recv-buffer-size)`
(`server.clj:450`) uses a constructor deprecated for removal — `Integer/valueOf`
is the drop-in — and `run-server`'s `:pre` checks `port` but not `:handler`
(`server.clj:442`), so a missing handler surfaces as an NPE inside the reactor
rather than at startup.

*jolt-tcp:* options are applied before `bind` in `jolt.net/listen`
(`jolt-net/src/jolt/net.clj:63-82`, `173-178`).

<a id="10"></a>
## 10. Queue admission is check-then-act

**Severity: low-medium (wrong exception type under concurrency).**

```clojure
;; server.clj:162-169 and 171-181
(when (zero? (.remainingCapacity control-queue))
  (throw (ex-control-queue-full)))
(.add control-queue [event callback])
```

`remainingCapacity` and `add` are separately atomic but not atomic together.
`write` serializes its own path under `socket-lock` (`server.clj:111`), but
`close`, `pause-reads` and `resume-reads` call `queue-write`/`queue-control`
**without** the lock (`server.clj:116-137`), and those are exactly the functions
documented as safe to call from anywhere. Two threads can both observe capacity
1 and both `add`; the loser gets `IllegalStateException: Queue full` from
`ArrayBlockingQueue.add` instead of the structured `::control-queue-full`
`ExceptionInfo` the docstrings promise. Callers matching on `:err` miss it, and
it lands in the generic handler-exception path.

`update-write-limit` (`server.clj:145-149`) has the same shape: the subtract and
the compensating add-back are not atomic together, so a concurrent producer can
transiently observe a negative credit and fail a write that should have fit.
That direction is at least fail-safe.

**Fix.** Use `offer` and branch on its result, and make the credit accounting a
single CAS loop.

<a id="11"></a>
## 11. Smaller observations

- **The callback thread depends on whether the write completed inline.**
  `write`'s fast path calls `(callback)` directly on the caller's thread while
  holding `socket-lock` (`server.clj:110-114`); the queued path submits it to
  the executor (`server.clj:298`). Callbacks therefore run on the caller's
  thread, under a lock, or on a pool thread, with no way to know which. Any
  callback that takes another lock has an ordering hazard, and a
  continuation-style writer recursing through the fast path builds stack
  proportional to how much the socket accepts in one go — bounded in practice by
  the send buffer filling, but not bounded by construction. Worth either
  documenting or always deferring.
- **`shutdown-key` discards `close-ex`** (`server.clj:373`) — see finding 4d.
- **No connection cap.** `handle-accept` accepts unconditionally; there is no
  maximum-connections option and no backpressure on the accept path, so fd
  exhaustion is reached by default. Given finding 1, `EMFILE` then stops the
  server rather than shedding load.
- **`Context` fields are `volatile!` mutated with `vswap!`**
  (`server.clj:251-266`), which is a non-atomic read-modify-write. It is safe
  only because of teensyp's serial-per-connection guarantee. That is a real
  invariant, worth stating in the `Context` docstring since the fields are
  reachable from handler code via the `Socket` protocol.

## Not upstream findings

For completeness, these jolt-tcp divergences are **not** criticisms of teensyp
— they exist because the host lacks a JVM facility, and the README documents
each:

- `teensyp.buffer/Buffer` replacing `java.nio.ByteBuffer` (no `java.nio`).
- Charset **name strings** instead of `Charset` objects.
- `teensyp.stream` exposing a `conn` protocol instead of `java.io` streams
  (jolt's `proxy` is `reify`, so `InputStream` cannot be subclassed).
- `try-write` always returning false: all socket I/O runs on the reactor thread
  under `jolt.net`, so the immediate-write fast path does not exist.
- No `:direct-read-buffer?` (no direct buffers).
- Masking `SIGPIPE` explicitly — the JVM already does this process-wide.

## Suggested filing order

1. **Finding 1** — server death from an ordinary `accept` error is the one that
   turns a bad minute into an outage.
2. **Finding 3** — a two-line fix, remotely triggerable, and it is Capra's
   problem too.
3. **Finding 2** — needs an API addition, so it is worth agreeing on the shape
   early.
4. **Findings 4, 5** — both are contract changes worth settling before more
   adapters are built on teensyp.
5. **Findings 6, 7, 9** — small, self-contained, independently landable.
6. The rest as cleanup.

Findings 3, 6, 7 and 9 are localized enough to arrive as patches. Findings 2, 4
and 5 change the public contract and probably want a design note first.
