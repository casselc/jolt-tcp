# Upstream improvements for jolt-tcp

This document records changes to Jolt, `jolt.ffi`, and the Jolt runtime that
would make jolt-tcp safer, smaller, or faster. It is a local planning document,
not an upstream issue tracker.

## Release refresh — 2026-08-25

This document's v0.4.15 probes are historical evidence, not a current Jolt
capability list. Official Jolt v0.7.27 now includes the fixes first requested
here for real CPU counts and monotonic time, `System/arraycopy`, SIGPIPE-safe
IPv4 sockets with peer information, variadic FFI, public fibers,
thread-correct `jolt.ffi/errno`, exact-width integers, scoped allocation,
declarative struct layouts, and struct-by-value calls. Use the released APIs
instead of copying the manual workarounds below.

jolt-net no longer depends on the proposal-only target descriptor or scoped
byte-array pointer loan. It now derives its exact host tuple from released APIs
and uses scoped scratch buffers with `read-into!`/sliced `write-array`, passing
all 213 checks on stock v0.7.27. A future pointer loan is an optimization only.
The concurrent-FFI stress history and the broader jolt-net lifecycle/capability
design also remain separate review obligations; a nearby released API does not
by itself close them.

## Verification baseline

Revalidated 2026-07-23 against:

- installed `joltc v0.4.15`;
- the Jolt checkout in [`refs/jolt`](../refs/jolt), commit
  `d5aaf503fc7a45c5638d21215eb153b426a7e8dc`; and
- the current local jolt-tcp source.

The vendored [`stdlib/jolt/ffi.clj`](../refs/jolt/stdlib/jolt/ffi.clj) remains a
thin macro surface over host-provided primitives. Recheck every claim against
the exact upstream SHA before removing a workaround.

The reviewed Jolt proposal fork is now published only to `casselc/jolt` on
`codex/upstream-improvements-6-8`; nothing has been pushed to the upstream
project's origin and no pull request has been opened. The exact core revision
used here is `85f645aa1178e4b631198dcbaf46bdad1283750b`. It includes scoped
byte-array ranges introduced at `1c8fdb97`, Windows path correction at
`358c42b7`, and the variadic FFI boundary introduced at `ecf7728f`. The
known-unsound runtime AOT prototype remains isolated on
`research/aot-v5-prototype` at `21062d5b` and is not part of the proposal
branch.

## Implementation update — 2026-07-24

- `jolt-tcp` production code now consumes the public `jolt.net` owned-handle,
  byte-slice, poller, endpoint, and lifecycle APIs. It has no direct
  `jolt.ffi`, `pollfd`, `fcntl`, pipe-wake, errno, or native-layout code.
- `deps.edn` pins `casselc/jolt-net` at
  `7de096d0f02f0f452124a110cbbd4f5b966f4c67`. That CI follow-up to the reviewed
  combined W1/W2 merge adds native POSIX descriptor validation without changing
  its API. The branch combines Windows W1 blocking runtime and W2 non-blocking
  byte-I/O evidence while preserving a fail-closed readiness boundary. It does
  not claim Windows TCP loopback support; the exact revision's hosted platform
  jobs remain candidate evidence until observed green.
- The TCP layer retains only transport-neutral endpoint maps and the diagnostic
  descriptor. Stable ownership generation and stale-readiness rejection remain
  `jolt.net` invariants rather than being duplicated here.
- TCP-specific proofs now cover accept/stop publication, executor rejection,
  bounded shutdown drain, outcome-aware write failure, task retention, EOF
  visibility, and recursive-lock exclusion. The runtime suite adds the matching
  real-loopback and forced-interleaving witnesses.
- `teensyp.client` now supplies the missing outbound blocking composition over
  `jolt.net`: one absolute monotonic connect deadline across resolver
  candidates, exact failed-attempt ownership rollback, persistent
  direction-specific pollers, partial-safe byte I/O, nil EOF, additive
  per-operation monotonic deadlines (including same-direction queue time), and
  idempotent half/full close without exposing descriptors. Its separate proof
  record has SAT controls, corrected bounded-UNSAT checks, and executable
  semantic oracles.

The remaining recommendations below should therefore be read as upstream
capability rationale and historical workaround evidence. A workaround already
deleted from `jolt-tcp` is not evidence that every other ecosystem consumer has
adopted the corresponding primitive.

## Priority summary

| Priority | Upstream area | Independently landable change | Main payoff |
| --- | --- | --- | --- |
| P0 | Jolt build | Closed-world, fresh-process AOT; selective runtime reuse remains research-only | Prevent server artifacts from mixing mutable compiler states |
| P1 | Jolt runtime | Reproduce and harden concurrent foreign calls | Prevent process corruption under concurrent socket work |
| P1 | `jolt.ffi` | Overlapping array copy and range-aware native byte transfer | Remove receive/send allocations and per-byte loops |
| P1 | `jolt.ffi` | Narrow integers | Delete `pollfd` half-word packing |
| P1 | `jolt.ffi` | Scoped allocation and call-boundary `errno` | Delete repeated native-lifetime and per-OS error scaffolding |
| P1 | Jolt runtime | Mask `SIGPIPE` by default | Remove a process-kill footgun |
| P2 | `jolt.host` | Complete target descriptor | Centralize ABI, platform, separator, and CPU facts |
| P2 | Jolt stdlib | Cross-platform `jolt.net` foundations | Make jolt-tcp an adapter over audited primitives |
| P2 | Jolt stdlib | Shared byte input/output protocols | Reuse connection/stream adapters across libraries |
| P2 | `jolt.ffi` | Struct layouts and by-value aggregates | Remove ABI-sensitive offsets and support broader native APIs |
| P3 | Jolt bytes | Contiguous storage and richer buffer operations | Improve the transport hot path further |
| P3 | `jolt.ffi` | Per-call-site `:blocking` | Remove duplicate blocking bindings |

P0 is a demonstrated wrong-code class. P1 removes a correctness or
process-safety risk. P2 is high-value consolidation. P3 can follow once the
smaller substrate is measured.

## 1. Replace selective runtime AOT reuse with a closed-world build

### Current constraint

The current Jolt loader keys a namespace cache artifact from that namespace's
own source length and Chez `equal-hash`, together with the Jolt version. This is
not collision-safe, and hashing only the consumer source would still be
insufficient.

A two-namespace v0.4.15 probe demonstrated the second failure:

1. `probe.main` called a macro from `probe.macros`; both compiled and the result
   was `:v1`.
2. Only the macro namespace changed, so the macro expanded to `:v2`.
3. The next run reported a cache hit for `probe.main`, a miss for
   `probe.macros`, and returned stale `:v1`.

jolt-tcp's README already warns about stale code observed from the current
collision-prone cache. TCP and HTTP code are not less exposed than a version
namespace: stale macro expansions, constants, protocol code, or FFI definitions
can all be compiled into an unchanged consumer.

The deeper review showed that a dependency manifest is not a complete boundary.
Live fixtures found compile-time reads of ordinary nonmacro Vars without
`require` edges, global type-registry changes, and pre-`ns` caller context.
Other failures crossed reader/compiler callbacks, aliases, retained namespace
cells, direct/nested loading, and selection-time mutation.

The checked-in 44-model Chiasmus/Z3 suite proves bounded gates only if every
consumed compiler input is observed completely, synchronously, and through
non-spoofable instrumentation. It does not prove instrumentation completeness.
See the cross-project
[`AOT proof record`](../../jolt-upstream/docs/aot-cache-provenance-invariants.md).

### Upstream change

Make production AOT a closed-world build:

- start a fresh compiler process and namespace image;
- resolve and digest the exact project graph;
- require each file's first meaningful form to declare the requested namespace;
- freeze readers, compiler/features, target, Jolt version, and declared native
  inputs;
- compile one snapshot into one immutable executable/image; and
- reject or fall back for dynamic compiler effects outside the graph.

Do not reuse the image against independently mutated live compiler/runtime
state. Whole-image content addressing is reasonable; selective in-process
namespace reuse remains research-only. A reusable native streaming SHA-256
primitive should provide whole-build identity rather than a cheap runtime hash.

### Acceptance criteria

- Same-length source changes produce different build identities and results.
- Macro, compile-time nonmacro Var, reader, alias, registry, target, or compiler
  changes rebuild the snapshot or fail closed before user forms execute.
- Source metadata and diagnostics describe the selected build sources.
- Dynamic load/reload outside the declared graph is rejected or takes the
  ordinary non-AOT path.
- Concurrent publication cannot mix namespace generations.
- An identical clean build deterministically reuses or reproduces one immutable
  image.

### jolt-tcp payoff

Server behavior can no longer silently lag behind resolved source. Project
documentation can stop treating cache deletion as a correctness mechanism for
the validated closed-world build. The current runtime cache warning must remain
for selective namespace loading.

## 2. Harden concurrent foreign calls independently of scheduler work

### Current constraint

jolt-tcp runs a poll reactor, handler workers, and a separate callback executor.
Those threads can call native socket and memory operations concurrently. A
related high-concurrency HTTP workload has ended in Chez's
`nonrecoverable invalid memory reference`, but the exact unsafe runtime
transition has not yet been reduced.

The current executor shims must not be used as evidence for or against that
failure. In both installed v0.4.15 and the vendored source,
`newCachedThreadPool`, `newVirtualThreadPerTaskExecutor`, and
`newWorkStealingPool` all map to one fixed 32-worker implementation. A live
40-task gated probe started exactly 32 tasks for the cached and virtual
constructors.

### Upstream change

Treat these as separate issues:

1. Check in a minimal concurrent-FFI stress program with exact SHA, host,
   foreign signature, `:blocking`/collect-safe setting, concurrency, and
   observed outcome. Fix any runtime corruption it proves.
2. Make JVM-named executor shims semantically honest—implement the named
   behavior, introduce Jolt-specific names and deprecate the misleading ones,
   or reject unsupported semantics clearly.
3. Design genuine M:N lightweight tasks only as a later feature/performance
   project.

### Acceptance criteria

- The reproducer survives repeated sanitizer/debug and optimized runs at a
  documented stress level on supported targets.
- The fix has a test that fails for the isolated bug, not merely for “too much
  load.”
- Constructor names and documented scheduling/queueing behavior agree.
- No safety fix depends on completion of a virtual-thread scheduler.

### jolt-tcp payoff

The transport can make an evidence-backed concurrency claim. Its callback
executor remains bounded policy rather than an accidental defense against an
uncharacterized runtime bug.

## 3. Add overlapping bulk byte copy and FFI range transfers

### Current constraint

Jolt has no `System/arraycopy`. In the vendored host,
`ffi/read-array` and `ffi/write-array` transfer bytes in per-byte loops and
offer no destination/source offset. Jolt byte arrays are vector-backed.

The receive path currently reads native data to a newly allocated byte array
and then copies it into the connection buffer. The send path copies a buffer
range to a temporary array and then writes that array to native memory. These
allocations and loops sit directly in the transport hot path.

### Upstream change

Add the small primitives first:

```clojure
(arraycopy src src-off dest dest-off len)
(ffi/read-array! ptr len dest dest-off)
(ffi/write-array ptr src src-off len)
```

`arraycopy` must have memmove-style overlap semantics. The FFI operations must
define bounds checks, zero-length behavior, signed byte representation, null
pointer behavior, and who owns the native memory. Their implementation should
use the fastest host representation available rather than round-tripping
through a temporary Jolt collection.

Contiguous bytevector-backed array storage is a worthwhile follow-on, but should
not block the API and correctness win above.

### Acceptance criteria

- Forward and backward overlapping self-copies agree with a simple reference
  implementation.
- Random native-memory subranges round-trip into arbitrary destination offsets.
- Zero-length and exact-end ranges work; invalid ranges fail before touching
  memory.
- jolt-tcp can receive into and send from an existing buffer range without an
  intermediate byte array.
- Benchmarks report allocation count and throughput before and after.

### jolt-tcp payoff

The reactor removes two hot-path allocation/copy stages, and
`teensyp.buffer` can implement compaction and range operations on a common
primitive.

### Local proposal status

Jolt proposal commit `3105198a` implements the overlap/range API and passes
17/17 focused correctness checks. It lets callers avoid intermediate Jolt
arrays, but the FFI host path still copies per byte and no before/after
allocation or throughput benchmark has been recorded. Treat the performance
criterion as open.

## 4. Add narrow integer types to `jolt.ffi`

### Current constraint

`jolt.ffi` has no 16-bit integer type. `struct pollfd` is
`{int fd; short events; short revents}`, so
[`teensyp.ffi-net`](../src/teensyp/ffi_net.clj) writes both 16-bit fields through
one 32-bit slot and masks/shifts `revents`.

That works only because the fields are adjacent, the assumed layout matches,
and the target byte order agrees with the packing. It is not a portable field
access API.

### Upstream change

Add signed and unsigned 8-, 16-, 32-, and 64-bit foreign types consistently to
`read`, `write`, callback signatures, and foreign calls. Names may include
`:int16`/`:uint16`; the important contract is exact width and signedness.

### Acceptance criteria

- Read/write and call/callback conformance tests cover extrema for every width.
- A C fixture confirms values and offsets on every supported target.
- `pollfd.events` and `pollfd.revents` can be accessed as separate fields.

### jolt-tcp payoff

The mask/shift workaround disappears immediately, even before a full struct
descriptor exists.

## 5. Add scoped allocation and call-boundary native errors

### Current constraint

[`teensyp.ffi-net`](../src/teensyp/ffi_net.clj) repeats “allocate → call/read →
free” for socket options, pipes, and client helpers. It binds
`__errno_location` on Linux and `__error` on macOS and reads it separately after
a failing call.

Allocation cleanup is easy to miss on exceptions. Error retrieval has a deeper
correctness constraint: `errno` is thread-local and can change on any
intervening native call, so a convenience function invoked later may report the
wrong failure.

### Upstream change

Land two independent facilities:

- lexical, exception-safe `with-alloc`, `with-out`, and C-string helpers; and
- an option for a foreign call to capture `errno` or the platform-native socket
  error immediately at the call boundary and return structured failure data.

An ordinary `ffi/errno` accessor is still useful for low-level code, but it is
not the complete safe error API.

### Acceptance criteria

- Native allocations are released on normal return and thrown Jolt/native
  errors.
- A test deliberately performs another native call after failure and proves
  the captured error remains the original one.
- Linux/macOS `errno` and Windows Winsock errors map to structured data without
  losing the native numeric code.

### jolt-tcp payoff

Native lifetimes become shorter and auditable, per-platform errno bindings
disappear, and socket failures can carry stable operation/endpoint/error
context.

## 6. Mask `SIGPIPE` by default

### Current constraint

A send to a closed peer can raise `SIGPIPE` and terminate the entire Jolt
process. jolt-tcp passes `MSG_NOSIGNAL` on Linux and sets `SO_NOSIGPIPE` on
accepted macOS sockets. Any future socket or pipe binding that forgets those
guards restores the process-fatal behavior.

### Upstream change

Match the JVM's process-level behavior by ignoring/masking `SIGPIPE` at Jolt
startup, or expose a documented signal-disposition API that libraries can use
once. Preserve `EPIPE` as ordinary structured call failure.

### Acceptance criteria

- Repeated writes after peer close never terminate the process.
- The caller receives the target's normal broken-pipe code.
- Child processes do not accidentally inherit a surprising disposition if Jolt
  promises otherwise.

### jolt-tcp payoff

Per-send flags and accepted-socket special cases disappear.

## 7. Expose a complete target descriptor

### Current constraint

jolt-tcp derives macOS from `System/getProperty "os.name"`. Other native
decisions need more: architecture, libc, ABI/calling convention, endianness,
pointer width, and CPU count. In current source,
`Runtime.availableProcessors` is hardcoded to `1`; a live v0.4.15 probe returns
`1`. `System/getProperty "os.arch"` is absent and host path/separator shims
contain POSIX assumptions.

### Upstream change

Expose a stable map such as:

```clojure
{:os :linux
 :arch :x86-64
 :abi :sysv-amd64
 :libc :glibc
 :endian :little
 :pointer-bits 64
 :file-separator "/"
 :path-separator ":"
 :available-processors 12}
```

Unknown facts should have an explicit unknown value. Do not infer ABI solely
from OS and architecture.

### Acceptance criteria

- Values agree with compiler/native facts on Linux x86_64, Windows x86_64, and
  macOS arm64.
- CPU count respects the host/container policy Jolt documents.
- The descriptor participates in closed-world build and native-artifact
  identity where relevant.

### jolt-tcp payoff

Socket layout/constants, native artifacts, and default executor sizing can use
one tested source of truth.

### Local proposal status

Jolt proposal commits `3105198a` and `34fabb2c` expose the zero-argument
`jolt.host/target` and replace fuzzy inference with an exact Chez machine-type
allowlist. The expanded focused suite passes 33/33 checks on Linux with
`scheme`; Windows x86_64 and macOS arm64 still need native validation.

## 8. Factor a cross-platform `jolt.net`

### Current constraint

jolt-tcp binds POSIX sockets, `fcntl`, `pipe`, and `poll` directly. It implements
a self-pipe wakeup and currently lacks general endpoint/DNS and peer-address
support.

Upstream Jolt is not starting from zero:

- `stdlib/jolt/mvn_http.clj` already contains `getaddrinfo`, address iteration,
  Windows Winsock startup, connection timeout, and TLS client work; and
- `jolt-core/jolt/nrepl.clj` contains server socket scaffolding.

A useful stdlib design must factor those implementations together with
jolt-tcp's non-blocking reactor. Simply moving its loopback POSIX adapter
upstream would cement the current platform limits.

### Initial contract

- generic local/remote endpoint values;
- DNS and IPv4/IPv6 candidate iteration;
- Linux, macOS, and Windows sockets;
- non-blocking accept/connect/read/write and half-close;
- local and peer address access;
- a wakeable readiness abstraction;
- timeout primitives suitable for connect/read/write/idle policies;
- default broken-pipe safety; and
- operation- and endpoint-rich structured errors captured at the native call
  boundary.

The readiness API should specify ownership, thread safety, cancellation, and
whether a wake is level- or edge-like. TLS may remain layered, but existing
`mvn_http` TLS must be able to migrate without losing capabilities.

Before deadline-bearing APIs land, upstream must correct `System/nanoTime`.
It currently derives from UTC wall-clock milliseconds, so it is neither
monotonic nor nanosecond-resolution. Non-blocking connect must return an
explicit in-progress state and verify `SO_ERROR` after writable readiness.
On Win64, socket handles and poll layouts must use pointer-width Winsock types
rather than POSIX fd assumptions. The first resolver contract should accept
ASCII and caller-provided IDNA forms, deferring automatic Unicode/
`GetAddrInfoW` policy.

### Acceptance criteria

- Echo/client fixtures pass over IPv4 and IPv6 on all supported hosts.
- DNS fallback tries usable address candidates and reports aggregate failure
  clearly.
- Another thread can wake a blocked poll and change interest without a lost
  wake.
- Peer/local address, EOF, half-close, timeouts, and closed-peer writes have
  cross-platform tests.
- Deadline math uses a real monotonic clock, and non-blocking connect tests
  distinguish readiness from the final `SO_ERROR` result.

### jolt-tcp payoff

jolt-tcp becomes a teensyp-compatible reactor and backpressure policy over
audited upstream transport primitives. jolt-http gains real remote addresses
through it.

## 9. Shared byte input/output protocols

### Current constraint

Jolt cannot subclass JVM `InputStream`/`OutputStream`. jolt-tcp therefore owns a
custom connection/stream contract, while jolt-http adds a request-body source
and response sink. An output-only abstraction would leave half the duplication.

### Upstream change

Define small Jolt-native byte input and output protocols with:

- byte-array range reads/writes;
- close and half-close;
- optional flush;
- explicit blocking/backpressure behavior; and
- adapters for arrays, files, buffers, and sockets.

Do not claim Java nominal type compatibility. These are protocols that Jolt
code and host adapters can implement.

### jolt-tcp payoff

Connection adapters become reusable by HTTP and future codecs without forcing
the reactor itself into a blocking stream model.

## 10. Struct layouts and by-value aggregates

Named layouts with size, alignment, field offsets, and typed reads/writes would
replace `sockaddr` and `pollfd` constants. Aggregate argument/return lowering is
a separate, larger ABI project needed directly by jolt-hegel.

Land layout access before by-value calls. Test both against a C conformance
fixture on every target. jolt-tcp benefits from the layout half even if the
foreign-call compiler work arrives later.

## 11. Per-call-site blocking behavior

`recv`, `send`, and `connect` currently need separate bindings for reactor
non-blocking calls and blocking test-client calls because `:blocking` belongs
to the binding. A safe call-site option or generated pair of wrappers would
remove those duplicates. It must preserve compile-time signature checking and
make collect-safe behavior visible in stack traces/diagnostics.

## Local lifecycle work completed

The local fork now:

- retains the reactor future and a shared completion;
- makes stop idempotent and bounded by `:stop-timeout-ms`;
- closes the listener and both wake-pipe descriptors and frees the per-server
  receive/send buffers exactly once from the reactor's outer `finally`;
- serializes ordinary wakes, the terminal stop wake, and wake-pipe retirement
  with an owner-independent CAS gate, preventing a write after descriptor close
  or reuse;
- rolls back every partial-start acquisition;
- uses an explicit `:start`/`:abort` handoff so a scheduled future cannot enter
  the reactor after constructor cleanup; and
- borrows caller-supplied executors by default across both failed construction
  and successful stop, while allowing explicit adoption through
  `:shutdown-executor?` and `:shutdown-callback-executor?`.

Focused lifecycle tests exercise wake/close ordering, start abort, partial
failure, repeated immediate start/stop, idempotence, and bounded timeout. The
full acceptance/property suite also passes.

Executor ownership is now explicit: server-created pools are always reaped,
supplied pools remain caller-owned unless their matching shutdown option is
true, and the same policy applies to startup rollback and normal stop.

The handler contract, backpressure policy, half-close response policy, and
reactor exception isolation likewise remain jolt-tcp concerns.

## Recommended implementation order

1. Specify the closed-world, fresh-process AOT build and whole-build digest. Do
   not ship the selective runtime namespace-cache prototype as the fix.
2. Reduce and fix concurrent-FFI safety independently of executor/scheduler
   semantics.
3. Add overlapping copy and range-aware native byte transfers.
4. Add narrow integers, scoped allocation, and captured native errors.
5. Add the target descriptor.
6. Mask `SIGPIPE`.
7. Factor `jolt.net` from existing upstream and jolt-tcp implementations.
8. Add shared byte stream protocols, then richer buffer/storage support.
9. Add struct layouts before by-value aggregates.
10. Add call-site blocking as an ergonomic cleanup.
