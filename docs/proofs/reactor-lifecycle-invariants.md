# Bounded reactor lifecycle invariants

Checked on 2026-07-23 with Chiasmus, Z3, Prolog, and the jolt runtime tests.

This note preserves the three arguments used to remove the stream/churn
quarantine: pending work has a generation-stable identity, EOF is exposed only
after all pre-EOF bytes are handed to the terminal invocation, and the write
path no longer recursively acquires its socket lock.

These are bounded source models plus executable runtime witnesses. They are not
a proof of the complete POSIX reactor or scheduler.

## 1. Stale pending work cannot select a reused fd

### Claim

If a pending event names `(fd, generation)`, the reactor processes it only when
the currently registered context has both the same fd and the same generation.
A stale event whose raw fd has been reused by a newer generation therefore
cannot select the replacement context.

### Source facts

- `src/teensyp/server.clj:84-88`: pending identity is `[fd generation]`.
- `src/teensyp/server.clj:440-458`: every new context receives a new generation.
- `src/teensyp/server.clj:536-547`: the pending drain checks generation before
  calling `handle-pending`.
- `src/teensyp/server.clj:394-410`: close finalization repeats the current-context
  check, removes the registry entry before `close(2)`, and performs the raw close
  only on the reactor.

The model uses fd and generation values in `0..3`. Its violation is:

```
same raw fd
and different generation
and selector_process
```

| Query | Expected | Result | Evidence |
| --- | --- | --- | --- |
| Exact `(fd, generation)` selector | no stale selection | **UNSAT** | core: `exact_identity_selector`, `violation_definition`, `queried_stale_event_processed` |
| Faulty fd-only selector | stale selection exists | **SAT** | event `(0,0)` selects current `(0,1)` |
| Exact-match non-vacuity | current work is admitted | **SAT** | both identities `(0,0)`, `selector_process=true` |

Models:

- [`models/fd-generation-safety.smt2`](models/fd-generation-safety.smt2)
- [`models/fd-generation-fd-only-control.smt2`](models/fd-generation-fd-only-control.smt2)
- [`models/fd-generation-nonvacuity.smt2`](models/fd-generation-nonvacuity.smt2)

## 2. EOF cannot hide bytes behind stream termination

### Claim

When bytes arrive after an older handler view was taken but before EOF, the
stream channel is closed only by the terminal handler invocation, after that
invocation pushes those late bytes. End-of-stream therefore cannot be exposed
while any modeled pre-EOF byte remains undelivered.

### Source facts

- `src/teensyp/server.clj:260-274`: `submit-read-handler` refreshes the view
  before setting `EOF-SEEN`, which is what `peer-eof-notified?` reports.
- `src/teensyp/server.clj:375-392`: EOF observed while a handler is `WORKING`
  schedules a terminal invocation once that handler finishes.
- `src/teensyp/stream.clj:84-102`: the invocation pushes all bytes in its view
  before closing the channel, and closes it on `peer-eof-notified?`, not raw
  `peer-closed?`.

The bounded model uses `old_bytes` in `0..8` and `late_bytes` in `1..8`.
It asks whether the channel can expose end-of-stream with:

```
delivered_bytes < old_bytes + late_bytes
```

| Query | Expected | Result | Evidence |
| --- | --- | --- | --- |
| Close after terminal push | no hidden byte | **UNSAT** | core includes `peer_eof_notified_orders_close_after_push`, delivery definition, and violation query |
| Faulty close on raw peer EOF | hidden byte exists | **SAT** | old=0, late=1, delivered=0, total=1 |
| Terminal-delivery non-vacuity | late bytes are actually admitted | **SAT** | old=1, late=1, delivered=2 |

Models:

- [`models/eof-byte-visibility.smt2`](models/eof-byte-visibility.smt2)
- [`models/eof-raw-peer-closed-control.smt2`](models/eof-raw-peer-closed-control.smt2)
- [`models/eof-byte-visibility-nonvacuity.smt2`](models/eof-byte-visibility-nonvacuity.smt2)

## 3. The write path has no recursive socket-lock acquisition

### Claim

The current path still reaches `set-flag!` from `write`, but only `write`
acquires `socket_lock`; `set-flag!` terminates in an atomic flag swap. The
pre-fix path acquired the same lock at both nodes.

### Source facts

- `src/teensyp/server.clj:151-159`: `write` owns the outer socket lock.
- `src/teensyp/server.clj:139-145`: `queue-write` reaches `set-flag!`.
- `src/teensyp/server.clj:90-102`: flags are atoms updated by `swap!`; there is
  no inner socket-lock acquisition.

The finite Prolog call graph gives:

| Query | Current | Pre-fix control |
| --- | --- | --- |
| `reaches(write, set_flag).` | one `true` answer | one `true` answer |
| `recursive_acquisition(write, set_flag, socket_lock).` | no answers | one `true` answer |

Models:

- [`models/flag-lock-current.pl`](models/flag-lock-current.pl)
- [`models/flag-lock-faulty-control.pl`](models/flag-lock-faulty-control.pl)

The historical contention witness completed only 143 of 801 critical sections
and emitted `not lock owner` errors. That observation is preserved in
`src/teensyp/server.clj:93-100`; the old stress harness itself is not retained,
so the source-graph control is the reproducible structural witness.

## Reproduction

The SMT files are Chiasmus-ready fragments; `chiasmus_verify` supplies solver
commands. Run `chiasmus_lint` and then `chiasmus_verify` with solver `z3`.
Expected statuses are listed above.

For the Prolog files, run `chiasmus_verify` with solver `prolog` and the two
queries shown in the table. An empty answer set for current recursive
acquisition is the expected result.

Runtime companions are:

- `test/teensyp/server_test.clj:330-374`: four assertions pin the EOF
  observation/notification boundary and byte conservation.
- `test/teensyp/server_test.clj:396-440`: nine assertions pin reactor-owned,
  exactly-once close and stale-generation rejection.
- `test/teensyp/server_property_test.clj:348-404`: arbitrary chunkings exercise
  stream framing and EOF in the default gate.

Run all of them with:

```sh
joltc -M:test
```

## Bounds, assumptions, and gaps

- The fd/generation proof models selection from the pending set. It does not
  model arbitrary native-memory corruption or external code calling `close(2)`.
- Generation is treated as non-wrapping in the modeled run. The implementation
  uses a monotonically incremented integer; practical numeric exhaustion is
  outside this proof.
- The EOF model abstracts bytes to counts and one “older view, late arrival,
  terminal view” transition. Framing and byte values remain the job of the
  runtime/property tests.
- The lock model is a source-derived call graph. It establishes absence of the
  old recursive acquisition path, not fairness or progress of the runtime's
  lock/atom implementations.
