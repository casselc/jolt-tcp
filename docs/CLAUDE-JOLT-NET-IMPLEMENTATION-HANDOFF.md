# Claude handoff: first `jolt.net` implementation slice

Use the following as the implementation prompt.

---

You are implementing the first bounded `jolt.net` slice in a local fork of
Jolt. Work independently through implementation, tests, review, and local
commits. Do not push anything, do not open a pull request, and do not modify
Jolt's `origin`; we do not own core Jolt. Ignore vendored `refs` directories.

## Repositories and fixed starting state

- Core implementation repo:
  `/home/chuck/ai-src/jolt-upstream`
- Start from branch `codex/upstream-improvements-1-5`, commit
  `287f9022` (`fix: unify host class registration`).
- Create a dedicated local branch and preferably a sibling worktree for this
  spike, for example `claude/jolt-net-stage1-2`. Do not work on or merge
  `research/aot-v5-prototype`; that branch is a known-unsound runtime AOT
  research artifact.
- Design/reference repo:
  `/home/chuck/ai-src/jolt-tcp`, branch `codex/jolt-upstream-fork`, design
  checkpoint `a6e96f2`.
- HTTP consumer reference:
  `/home/chuck/ai-src/jolt-http`, branch `codex/jolt-upstream-fork`, checkpoint
  `b941b48`.

Read these completely before coding:

1. `/home/chuck/ai-src/jolt-tcp/docs/JOLT-NET-DESIGN-SPIKE.md`
2. `/home/chuck/ai-src/jolt-tcp/docs/UPSTREAM-IMPROVEMENTS.md`
3. `/home/chuck/ai-src/JOLT-UPSTREAM-IMPROVEMENTS.md`
4. Core source:
   - `stdlib/jolt/mvn_http.clj`
   - `jolt-core/jolt/nrepl.clj`
   - `stdlib/jolt/ffi.clj`
   - the host FFI/target implementation under `host/chez`
5. jolt-tcp source:
   - `src/teensyp/ffi_net.clj`
   - the lifecycle and ownership paths under `src/teensyp`

There is a locked Claude review worktree at
`/home/chuck/ai-src/jolt-tcp/.claude/worktrees/jolt-net-doc-review`. Inspect it
read-only for history if useful. It is clean, and its substantive design diff
was already incorporated into the main jolt-tcp branch; do not modify, delete,
unlock, or reuse it for implementation.

## Accepted design decisions

Treat the eight accepted decisions in `JOLT-NET-DESIGN-SPIKE.md` as fixed:

- dependency-free socket core in `jolt.net`; TLS remains an optional layer;
- tagged values for would-block/EOF and structured exceptions for failures;
- opaque, idempotently closed owned handles with diagnostic raw access;
- a connected UDP socket wake pair for a future Windows `WSAPoll` backend;
- configurable IPv6 dual-stack behavior with the OS default unchanged;
- blocking DNS is documented as non-cancellable in the first resolver;
- retain the existing TLS native-library candidates temporarily; and
- jolt-tcp borrows caller-supplied executors unless shutdown ownership is
  explicitly adopted.

V1 hostname input is ASCII DNS or caller-supplied IDNA A-label form plus numeric
IPv4/IPv6. Reject other Unicode structurally. Do not silently feed UTF-8 to
Windows ANSI `getaddrinfo`, and do not invent a `GetAddrInfoW` policy in this
slice.

## Implement only this slice

Implement the non-deadline parts of Stage 1 plus the narrow Stage 2
endpoint/resolver substrate in core Jolt:

1. Add target-selected socket constants, handle types, C signatures, and
   structure layouts for the documented Linux x86-64, Windows x86-64, and macOS
   arm64 targets. Use the public zero-argument `jolt.host/target` introduced by
   the local fork. Tables must fail closed for unknown targets; do not infer an
   ABI or layout from a similar target name.
2. Add immediate native error capture:
   - POSIX `errno` immediately after the failing call;
   - `WSAGetLastError` immediately on Windows; and
   - the `getaddrinfo` return code retained and formatted with the appropriate
     resolver error function.
   Cleanup or formatting must not overwrite the captured error. Expose stable,
   structured `ExceptionInfo` data.
3. Add endpoint and resolved-address values for:
   - wildcard/host plus port;
   - numeric IPv4;
   - numeric IPv6, including scope ids where available; and
   - ASCII/IDNA-form DNS names.
4. Extract resolver iteration from `jolt.mvn-http`. Use `AF_UNSPEC`, preserve
   resolver order, copy every needed `addrinfo`/sockaddr field into owned Jolt
   data, and always call `freeaddrinfo`. No native pointer may escape the
   resolver scope.
5. Add numeric local and peer endpoint inspection using
   `getsockname`/`getpeername` and numeric formatting only—never reverse DNS.
   Include actual port-zero bind results.
6. Add the smallest owned socket abstraction needed to test those operations.
   Close must be idempotent. On Win64, `SOCKET` is pointer-width unsigned and
   `INVALID_SOCKET` is the all-bits-one value, not an `int` or a `<= 0` test.
7. Reuse the checked range transfer primitives from `3105198a`; do not add
   temporary Jolt array slices. Be honest that the current host FFI range loop
   is still per-byte and unbenchmarked.

Put the public dependency-free surface in `jolt.net` (with private supporting
host code where genuinely needed). Prefer direct `jolt.ffi` bindings and small
target tables over a new C shim.

## Monotonic-clock boundary

Current `System/nanoTime` is wall-clock milliseconds multiplied to look like
nanoseconds. Never use it for deadlines.

Either:

- implement and characterize a real monotonic host clock first, make
  `System/nanoTime` project it, and commit that prerequisite separately; or
- omit every deadline/timeout-bearing API from this slice and leave a precise
  blocker.

Do not fake monotonic behavior with UTC time. Do not implement connect, read,
write, poll, or TLS deadline promises until the monotonic tests pass.

## Explicitly out of scope

Do not implement in this pass:

- the POSIX or Windows poller/reactor;
- wakeup, cancellation, or non-blocking connect completion;
- TLS or OpenSSL adapters;
- jolt-tcp or jolt-http migration;
- shared high-level byte stream protocols;
- resolver worker pools or cancellable DNS;
- automatic IDNA conversion;
- generic native-artifact downloads;
- AOT cache changes; or
- unrelated legacy host-shim migrations.

If a tiny private readiness helper is unavoidable for a loopback test, do not
publish it as the poller design.

## Tests and evidence

Add focused tests for:

- exact platform layout/signature selection and unknown-target failure;
- Win64 high-bit handles and exact `INVALID_SOCKET`;
- copied resolver results surviving `freeaddrinfo`;
- resolver order, failed resolution, and non-ASCII rejection;
- IPv4 and IPv6 numeric loopback where supported;
- wildcard/port-zero bind and numeric local/peer round trips;
- injected failures proving error capture precedes cleanup;
- idempotent close and partial-acquisition rollback; and
- the monotonic clock, if implemented: non-decreasing under concurrency, finer
  than the old millisecond projection where the platform supports it, and
  independent of wall-clock adjustment.

Platform-selection tests must run on Linux without pretending that they are
native Windows/macOS validation. Mark native cross-platform coverage still
required.

The current Chez commands both resolve to `10.5.0-pre-release.1`; prefer
`scheme` in the validation commands. Disable the selective runtime AOT cache
during tests:

```sh
CHEZ=scheme JOLT_AOT_CACHE=0 make targetfacts hostclass ffi unit manifestcheck
CHEZ=scheme JOLT_AOT_CACHE=0 make ffistress executorprobe
```

Run the new focused network targets as well. Preserve these current baselines:

- target facts 33/33;
- host-class coherence 22/22;
- FFI/ranges 17/17;
- unit 1076/1076;
- manifest check passing;
- concurrent FFI 64 workers × 200 iterations completes `:ok`; and
- cached, virtual-per-task, and work-stealing executor shims each start 32/40
  gated tasks.

## Working and handoff rules

- Keep the worktree clean except for the bounded task.
- Preserve unrelated user changes.
- Use small conventional local commits, ideally separating:
  1. monotonic clock, if implemented;
  2. platform/error/ownership substrate;
  3. endpoints/resolver/inspection; and
  4. tests/docs.
- Do not push, open a PR, or contact upstream.
- Do not claim Windows/macOS native support from table tests alone.
- Before finishing, perform a skeptical source review of ownership, error
  capture timing, Win64 handle width, resolver lifetime, and public API scope.
- Report exact commit hashes, changed files, commands/results, remaining
  platform gaps, and any design decision that could not be resolved from the
  accepted document. Ask the maintainer only if proceeding would freeze a new
  public policy outside those accepted decisions.

---
