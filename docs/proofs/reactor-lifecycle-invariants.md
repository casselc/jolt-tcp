# Bounded reactor lifecycle invariants

Checked on 2026-07-24 with Chiasmus/Z3 against the migrated `jolt.net`
implementation and its executable TCP witnesses.

These are bounded source models, not a proof of the complete POSIX reactor,
Chez scheduler, or `jolt.net`. Successful legacy-callback progress remains
conditional on a finite write completing and on explicit fairness and handler
termination assumptions. A separate bounded claim now covers native write
failure for the failure-aware `write-completion` API; the retained legacy
success-only callback limitation is modelled separately rather than hidden.

## 1. Identity and close ownership now stop at the `jolt.net` API

### Claim

`jolt-tcp` does not use a raw descriptor as connection identity and does not
perform a raw close. A live connection is selected through the current
`jolt.net` registration token and the exact `Context` stored for the token's
owned-handle generation. Retirement removes the current token and closes the
owned `jolt.net` socket. `:fd` is diagnostic data only.

### Source facts

- `src/teensyp/server.clj:143-145`: pending work carries the generation copied
  from the `jolt.net` registration token, never an fd.
- `src/teensyp/server.clj:699-702`: `current-context?` requires that the
  registry still contains the identical `Context`.
- `src/teensyp/server.clj:785-869`: a context is built from the owned socket and
  token returned by `net/register!`; complete metadata precedes registry/handler
  exposure, and pre-publication registration/cancellation failures roll back.
- `src/teensyp/server.clj:1071-1099`: interest updates replace the stored token
  with the successor returned by `net/update-registration!`.
- `src/teensyp/server.clj:1140-1164`: normal readiness resolves the token's
  generation and then requires the complete event token to equal the current
  token. Shutdown repeats the same check at `src/teensyp/server.clj:969-985`.
- `src/teensyp/server.clj:734-748` and `995-1021`: retirement first calls
  `net/remove-registration!` with the current token and then `net/close!` with
  the owned socket. There is no `close(2)` binding in this layer.
- `src/teensyp/server.clj:19-20` and `785-810`: the native handle is retained
  only in `socket-info` for diagnostics.

The lower-layer claims are deliberately not duplicated here. `jolt.net`
rejects stale/foreign tokens, gives an acknowledged interest update a successor
revision, makes owned-handle close idempotent, and defers native close until
short leases drain. Their models and runtime controls live in
[`jolt-net`'s socket invariant record](https://github.com/casselc/jolt-net/blob/main/docs/proofs/socket-invariants.md).

The former `fd-generation-*.smt2` files were removed because they modelled an
obsolete local `(fd, generation)` selector and raw-close boundary. Keeping them
would claim that `jolt-tcp` still owns a safety property now provided by the
dependency.

The executable boundary witness is
`test/teensyp/server_test.clj:398-430`: it requires an owned `jolt.net` socket,
token/context generation agreement, exact registry identity, real endpoints,
and treats the fd as diagnostics.

## 2. Handler admission cannot strand `WORKING` or self-deadlock stop

### Bounded claim

At the observation boundary immediately after `submit-handler!` or its
pre-marked accept-path helper returns:

1. if `WORKING` remains true, `.execute` admitted the task whose `finally` will
   clear it; and
2. if `.execute` rejected synchronously, the catch path has requested close and
   cleared `WORKING`.

The model has exactly two executor outcomes, `admitted` and `rejected`. An
admitted task may or may not have completed its `finally` before the observation
boundary. A rejected task cannot have run that `finally`.

The asserted counterexample is:

```text
(WORKING after return and task not admitted)
or
(synchronous rejection and (WORKING after return or close not requested))
```

The accept path has an additional re-entrancy obligation. `Executor` permits a
direct implementation whose `execute` method runs the submitted handler inline.
The Context must therefore reserve `WORKING` before registry publication, but
the accept/stop CAS gate must be released before `.execute`. The asserted
counterexample is a published unreserved Context, executor invocation while the
gate is held, an inline stop unable to acquire that gate or reach its bounded
wait, or a stop-between-publication-and-submission trace without the reservation.

### Source facts

- `src/teensyp/server.clj:486-489`: every admitted task's `finally` calls
  `finish-work!`, which clears `WORKING`, republishes the context, and wakes the
  reactor.
- `src/teensyp/server.clj:491-517`: `submit-marked-handler!` owns executor
  admission and rejection cleanup; `submit-handler!` first publishes `WORKING`
  for ordinary read dispatch. The success path wraps the handler in that
  `finally`; synchronous rejection reports the error, requests close, calls
  `finish-work!` itself, and returns false.
- `src/teensyp/server.clj:830-869`: accept registers and constructs metadata,
  reserves `WORKING`, and publishes the Context under the short
  owner-independent accept/stop gate. It then releases that gate before calling
  `submit-marked-handler!`. Stop may linearize in the gap, but cleanup observes
  the already-published Context as active.

### Checked models

| Model | Expected | Chiasmus/Z3 result | Evidence |
|---|---:|---:|---|
| [`handler-admission-buggy.smt2`](models/handler-admission-buggy.smt2) | SAT | **SAT** | `submit_outcome=rejected`, `task_admitted=false`, `working_after_return=true`, `rejection_close_requested=false`, `violation=true` |
| [`handler-admission-corrected.smt2`](models/handler-admission-corrected.smt2) | UNSAT | **UNSAT** | core: `executor_outcome_defines_admission`, `admitted_task_or_rejection_clears_working`, `synchronous_rejection_requests_close`, `violation_definition`, `violation_query` |
| [`handler-admission-nonvacuity.smt2`](models/handler-admission-nonvacuity.smt2) | SAT | **SAT** | admitted case remains `WORKING=true`; rejected case has `WORKING=false`, `close_requested=true` |
| [`accept-inline-executor-stop-buggy.smt2`](models/accept-inline-executor-stop-buggy.smt2) | SAT | **SAT** | direct executor runs under the gate, so inline stop has `stop_gate_acquired=false`, `bounded_wait_reached=false`, and `self_deadlock=true` |
| [`accept-inline-executor-stop-corrected.smt2`](models/accept-inline-executor-stop-corrected.smt2) | UNSAT | **UNSAT** | core includes pre-publication reservation, gate-before-submission release, inline gate acquisition, bounded-wait reachability, stop-gap protection, and the violation query |
| [`accept-inline-executor-stop-nonvacuity.smt2`](models/accept-inline-executor-stop-nonvacuity.smt2) | SAT | **SAT** | the direct handler is reachable with `WORKING=true`, executor outside the gate, stop gate acquired, and bounded wait reached |

The executable companion at `test/teensyp/server_test.clj:913-957` checks both
a synthetic rejecting executor and a shutdown Jolt `ExecutorService`, requiring
error reporting, close-arity delivery, context retirement, and non-wedging
stop. The deterministic direct-executor witness at
`test/teensyp/server_test.clj:772-828` additionally runs the accept handler
inline; that handler calls the public stop function and must reach its
structured `::stop-timeout` boundary without a watchdog releasing
`accept-gate`. Reactor cleanup then completes.

This proof is admission safety, not executor liveness. It intentionally permits
an admitted task to remain `WORKING`; shutdown progress for such a task is the
separate conditional claim below.

## 3. Completed task history is not retained at cleanup

### Bounded claim

For zero to four active callback/close tasks when cleanup freezes a tracker:

1. freezing changes `:accepting?` to false;
2. every later registration attempt is rejected;
3. each task completion removes its own promise from `:active`; and
4. if every task active at freeze completes within the bound, cleanup observes
   a stable empty set. After the freeze, the set can only shrink.

The asserted counterexample is an open tracker after freeze, a late admitted
task, or a non-empty active set after every pre-freeze task completed.

### Source facts

- `src/teensyp/server.clj:51-70`: a tracker contains only `:accepting?` and the
  current `:active` set. Registration uses one CAS and rejects a closed tracker;
  completion removes exactly its own promise with `disj`.
- `src/teensyp/server.clj:72-105` and `704-733`: callback and handler-close
  submissions add a fresh completion promise before executor admission and
  remove it in `finally`, including the synchronous rejection fallback.
- `src/teensyp/server.clj:903-915`: cleanup closes admission before reading and
  awaiting the active set, and repeats until that set is empty.
- `src/teensyp/server.clj:1001-1030`: all close/callback producers are submitted
  before the two trackers are frozen and awaited.

### Checked models

| Model | Expected | Chiasmus/Z3 result | Evidence |
|---|---:|---:|---|
| [`task-tracker-retention-buggy.smt2`](models/task-tracker-retention-buggy.smt2) | SAT | **SAT** | two completed promises remain in the faulty append-only history, so `active_at_boundary=2` |
| [`task-tracker-retention-corrected.smt2`](models/task-tracker-retention-corrected.smt2) | UNSAT | **UNSAT** | closed admission rejects the late attempt and completion removal forces the bounded final active count to zero |
| [`task-tracker-retention-nonvacuity.smt2`](models/task-tracker-retention-nonvacuity.smt2) | SAT | **SAT** | two active tasks remain reachable before freeze, both complete, the late registration is rejected, and the final set is empty |

The executable companion at `test/teensyp/server_test.clj:496-548` submits and
completes 100 callbacks plus 100 handler-close tasks, requires both active sets
to be empty rather than historical, then proves freeze waits for one deliberately
blocked active callback and reaches the closed stable-empty state after release.
It also submits one post-freeze callback, requires the synchronous fallback to
complete it, and verifies that this cannot reopen or grow the tracker.

This is a bounded retention/lifecycle argument, not an executor fairness proof.
The corrected UNSAT query assumes every task active at freeze completes within
the bound; public stop's timeout remains the runtime boundary for a task that
does not terminate.

## 4. Stop resolves active writes before retirement

### Bounded claim

For one current, open `Context` already `WORKING` with a finite queued Buffer:

1. the owner-independent admission-state CAS is the `request-stop!`
   linearization boundary: an ordinary reactor action that increments `:active`
   first may finish after the CAS, but an already-returned native event batch
   cannot acquire a new accept/read reactor action once the phase is
   `:stopping`;
2. cleanup then retires the listener and removes read interest before the
   shutdown drain;
3. while the context is `WORKING+WRITING`, its only readiness interest is
   `:write`, and the reactor continues servicing that write;
4. after full write success, the accepted zero-arg callback can release the
   handler; after native write error, every queued `write-completion` is settled
   with `{:status :failed :exception ex}` before retirement and can release a
   handler waiting on that outcome;
5. the released handler terminates and clears `WORKING`, and CLOSED contexts
   have empty readiness interests; and
6. only then does cleanup retire the registration and owned socket.

Here “admission” primarily means acquisition of the ordinary reactor-action
gate. A pre-admitted `process-pending!` action may complete a dispatch after the
CAS, and the single reactor cannot enter cleanup until that action returns.
Accept publication has a stronger boundary: `do-accept` may have accepted and
registered a socket, but the `WORKING` reservation plus registry publication
shares a short owner-independent gate with the stop CAS. Publication therefore
linearizes wholly before stop, or stop wins and the registration/socket is
rolled back. Executor submission occurs after gate release. Stop may linearize
between publication and submission, but it sees the Context as `WORKING` and
cleanup waits for its admission/rejection path. The finite shutdown model
summarizes the outer
action-acquire/stop ordering, an
action remaining active across the stop CAS and releasing without reopening
admission, listener/read revocation, zero or more partial-write
readiness/service cycles, successful callback execution, handler `finally`,
and final retirement. The asserted violation is any post-CAS acquisition of an
accept/read reactor action, active ordinary work remaining when the single
reactor reaches cleanup, admission reopening on release, listener/read
admission remaining in the drain, retirement while `WORKING`, or failure to
service/callback/quiesce/retire within the bound when every progress assumption
below holds.

### Source facts

- `src/teensyp/server.clj:176-228`: stop changes the shared admission atom from
  `{:phase :open}` to `{:phase :stopping}` with CAS. Ordinary actions CAS
  increment `:active` only while the current phase is open and decrement it in
  `finally`; release preserves the stopping phase. No thread-owned lock is used.
- `src/teensyp/server.clj:1124-1164`: pending processing and every event from an
  already-returned `await-ready` batch must acquire that admission before
  `do-accept` or connection dispatch.
- `src/teensyp/server.clj:176-201` and `816-869`: stop and accept publication
  share a CAS gate. A stop-first trace cannot publish; the accept cleanup path
  removes any registration and closes the accepted socket. A publication-first
  trace reserves `WORKING` before exposure and invokes the executor only after
  releasing the gate.
- `src/teensyp/server.clj:929-967`: shutdown preparation drains worker
  publications, processes active-handler controls, and services only `:write`
  events. A write exception runs the failure settlement path before close.
- `src/teensyp/server.clj:969-985`: the reactor repeats preparation and
  `net/await-ready` while any context remains `WORKING`, with the same current
  generation/token checks used by the ordinary loop.
- `src/teensyp/server.clj:1082-1099`: shutdown interests are exactly
  `#{:write}` only while `WRITING` and not CLOSED, otherwise `{}`; `:read` is
  always absent.
- `src/teensyp/server.clj:995-1030`: cleanup retires the listener first, calls
  `quiesce-handlers!`, and only then retires connection registrations/sockets
  and schedules close arities.
- `src/teensyp/server.clj:650-678`: a legacy Buffer callback is submitted only
  after all bytes have been written; a `write-completion` promise is likewise
  settled `:written` only after the Buffer is exhausted.
- `src/teensyp/server.clj:271-296` and `544-602`: close atomically revokes write
  admission and waits for already-admitted producers before the reactor drains
  the stable queue. The drain returns remaining byte credit, settles every
  queued outcome `:failed`, preserves the legacy success-only rule, and only
  then proceeds to retirement.
- `src/teensyp/stream.clj:42-54`: the blocking stream adapter waits on
  `write-completion` and throws the recorded native exception on failure.

### Explicit progress assumptions

The successful-write UNSAT result assumes all of the following within the
finite bound:

- the handler was active before stop and is current/not already closed;
- its queued Buffer is finite and ultimately completes successfully;
- the peer drains its receive side;
- the kernel eventually reports write readiness;
- the reactor thread is scheduled to service that readiness;
- the callback executor eventually runs an accepted success callback; and
- once that callback releases the handler, the handler terminates and runs its
  `finally`.

These assumptions are named in the model. Atom/CAS linearizability and the
queue operations' source semantics are also treated as runtime primitives; the
model does not attempt a weak-memory proof.

The write-error UNSAT result instead assumes a current `WORKING+WRITING`
context, fair reactor service of the failing write, a handler waiting on
`write-completion`, and termination after that promise is settled. It does not
assume peer draining, kernel write readiness after the failing service, or
callback-executor fairness: outcome promises are delivered by the reactor
before retirement.

### Checked models

| Model | Expected | Chiasmus/Z3 result | Evidence |
|---|---:|---:|---|
| [`accept-publication-stop-buggy.smt2`](models/accept-publication-stop-buggy.smt2) | SAT | **SAT** | a split open check goes stale, so stop wins before publication but `context_published=true`, `handler_submitted=true`, and rollback is false |
| [`accept-publication-stop-corrected.smt2`](models/accept-publication-stop-corrected.smt2) | UNSAT | **UNSAT** | the shared gate admits only `publication_first` or `stop_first`; no post-stop publication, stop-first resource escape, or published Context without `WORKING` exists |
| [`accept-publication-stop-nonvacuity.smt2`](models/accept-publication-stop-nonvacuity.smt2) | SAT | **SAT** | the stop-first trace reaches `phase=stopping`, no publication/reservation/handler, and registration rollback |
| [`stop-admission-inflight-buggy.smt2`](models/stop-admission-inflight-buggy.smt2) | SAT | **SAT** | a split check/increment uses a stale `:open` observation after `admission_phase_after_stop=stopping`, admitting both accept and read |
| [`shutdown-drain-buggy.smt2`](models/shutdown-drain-buggy.smt2) | SAT | **SAT** | immediate retirement makes `write_interest=false`, `write_serviced=false`, `callback=false`, `WORKING=true`, `retired_while_working=true`, `progress_stalled=true` |
| [`shutdown-drain-corrected.smt2`](models/shutdown-drain-corrected.smt2) | UNSAT | **UNSAT** | no post-stop reactor-action acquisition, early retirement, or successful-write stall exists under the named assumptions |
| [`shutdown-drain-nonvacuity.smt2`](models/shutdown-drain-nonvacuity.smt2) | SAT | **SAT** | a pre-stop action remains active across the CAS, later releases to `active=0` without reopening; the in-flight batch is not newly admitted; the write/callback/quiesce/retire path remains reachable |
| [`shutdown-drain-write-error-corrected.smt2`](models/shutdown-drain-write-error-corrected.smt2) | UNSAT | **UNSAT** | after fair service returns `write_error`, no stable-drain/outcome/quiesce/readiness/retirement stall exists for an outcome waiter |
| [`shutdown-drain-write-error-nonvacuity.smt2`](models/shutdown-drain-write-error-nonvacuity.smt2) | SAT | **SAT** | closed admission has zero active producers, failure settlement occurs at milestone 2, retirement at milestone 3, and the repaired trace is reachable |

The exact UNSAT core was:

```text
stop_fixture
active_handler_fixture
queued_write_fixture
current_context_fixture
open_context_fixture
in_flight_native_batch_fixture
pre_stop_admitted_event_fixture
read_ready_fixture
read_was_registered_at_stop
peer_eventually_drains
kernel_eventually_reports_write_ready
reactor_eventually_services_write_ready
callback_executor_eventually_runs_accepted_callback
released_handler_eventually_terminates
finite_write_succeeds_within_bound
stop_cas_closes_admission
single_reactor_completes_pre_admitted_before_cleanup
active_admissions_at_cleanup_definition
release_preserves_stopping_phase
accept_requires_open_admission_cas
read_requires_open_admission_cas
stop_retires_listener
stop_revokes_read_interest
retirement_follows_handler_quiescence
write_only_interest_definition
bounded_write_readiness_definition
bounded_write_service_definition
full_write_success_definition
success_only_callback_definition
callback_fairness_definition
handler_release_definition
handler_termination_definition
working_at_bound_definition
final_retirement_definition
retirement_violation_definition
progress_stalled_definition
violation_definition
violation_query
```

The executable success-path witnesses are:

- `test/teensyp/server_test.clj:671-770`: accept work is batch-bounded;
  cancellation and registration failure roll resources back; accept-first
  publication completes before stop, while no publication begins after the stop
  CAS.
- The adjacent direct-executor witness calls public stop from the inline accept
  handler, reaches the 25 ms structured timeout rather than spinning on the
  gate, and then observes complete reactor cleanup.
- `test/teensyp/server_test.clj:959-1000`: a deterministic CAS witness proves a
  pre-stop admission may remain active after the stop CAS, post-CAS acquisition
  is rejected, and release decrements `:active` without reopening the phase.
- `test/teensyp/server_test.clj:1002-1065`: a 1 MiB backpressured write proves
  stop remains waiting, then write readiness, the success callback, handler
  completion, and close-last ordering all occur after the client drains.
- `test/teensyp/server_test.clj:1067-1103`: stop waits for an active non-writing
  handler to return before invoking its close arity, and a late worker wake
  after poller close is harmless.

### Repaired write-error path and retained legacy edge

[`shutdown-drain-write-error-corrected.smt2`](models/shutdown-drain-write-error-corrected.smt2)
asks for a stalled `write-completion` waiter after a fairly serviced native
write error. Chiasmus/Z3 returned **UNSAT**. Its core includes the source
transition from write error to closed admission, the zero-active-producer
barrier, stable queue drain, failure settlement, handler release/termination,
empty CLOSED readiness, settlement-before-retirement milestones, and the
counterexample query. The paired non-vacuity model is **SAT** with:

```text
write_admission_after_failure=closed
active_write_producers_at_drain=0
stable_queue_drain=true
outcome_settled=true
WORKING at bound=false
write_interest at bound=false
retired at bound=true
outcome_step=2
retirement_step=3
```

[`shutdown-drain-write-error-edge.smt2`](models/shutdown-drain-write-error-edge.smt2)
remains an additional **SAT** limitation witness, not the buggy control. It now
applies specifically to callers that continue to block solely on the
backward-compatible zero-arg success callback. With the correct retirement
order and fair readiness/service, `write_outcome=write_error` produces:

```text
close_requested_after_service=true
callback_eligible=false
callback_runs=false
WORKING at bound=true
retired at bound=false
```

That remains the actual legacy callback contract. Code that must make progress
after either outcome must use `write-completion`; `write` callbacks intentionally
did not acquire a new failure arity in this backward-compatible repair.

The executable companion at
`test/teensyp/server_test.clj:848-911` injects a deterministic native write
exception. It requires the exact exception to settle the outcome, the legacy
callback to remain uncalled, the waiting handler to clear, and `stop-server` to
complete. The same test also checks the successful outcome value. The focused
admission test at `test/teensyp/server_test.clj:622-669` closes a Context with
two different exceptions, requires the first to remain authoritative, and
verifies that a later `write-completion` still throws synchronous
`::socket-closed` while exposing that first failure through `ex-cause`. The
failure path records the cause before it closes write admission, so the
synchronous error cannot observe closed admission before the cause is visible.

## 5. EOF cannot hide bytes behind stream termination

### Claim

When bytes arrive after an older handler view was taken but before EOF, the
stream channel closes only from the terminal handler invocation, after that
invocation pushes the late bytes. End-of-stream therefore cannot be exposed
while any modelled pre-EOF byte remains undelivered.

### Source facts

- `src/teensyp/server.clj:519-531`: `submit-read-handler` refreshes the view
  before setting `EOF-SEEN`, which is what `peer-eof-notified?` reports.
- `src/teensyp/server.clj:604-648`: EOF observed while a handler
  is `WORKING` schedules a terminal invocation after that handler finishes.
- `src/teensyp/stream.clj:92-110`: the invocation pushes all bytes in its view
  before closing the channel, and closes it on `peer-eof-notified?`, not raw
  `peer-closed?`.

The bounded model uses `old_bytes` in `0..8` and `late_bytes` in `1..8`.
It asks whether the channel can expose end-of-stream with:

```text
delivered_bytes < old_bytes + late_bytes
```

| Query | Expected | Result | Evidence |
|---|---:|---:|---|
| [`eof-byte-visibility.smt2`](models/eof-byte-visibility.smt2) | UNSAT | **UNSAT** | close-after-terminal-push prevents hidden bytes |
| [`eof-raw-peer-closed-control.smt2`](models/eof-raw-peer-closed-control.smt2) | SAT | **SAT** | `old=0`, `late=1`, `delivered=0` |
| [`eof-byte-visibility-nonvacuity.smt2`](models/eof-byte-visibility-nonvacuity.smt2) | SAT | **SAT** | `old=1`, `late=1`, `delivered=2` |

Runtime companions are `test/teensyp/server_test.clj:338-394` and the
generative stream property at
`test/teensyp/server_property_test.clj:347-405`.

## 6. The write path has no recursive socket-lock acquisition

### Claim

The current path still reaches `set-flag!` from `write`, but only `write`
acquires `socket-lock`; `set-flag!` terminates in an atomic flag swap. The
pre-fix path acquired the same lock at both nodes.

### Source facts

- `src/teensyp/server.clj:361-369`: `write` owns the outer socket lock.
- `src/teensyp/server.clj:318-358`: `queue-write` reaches `set-flag!`.
- `src/teensyp/server.clj:233-242`: flags are atoms updated by `swap!`; there
  is no inner socket-lock acquisition.

The finite Prolog call graph gives:

| Query | Current | Pre-fix control |
|---|---:|---:|
| `reaches(write, set_flag).` | one `true` answer | one `true` answer |
| `recursive_acquisition(write, set_flag, socket_lock).` | no answers | one `true` answer |

Models:

- [`flag-lock-current.pl`](models/flag-lock-current.pl)
- [`flag-lock-faulty-control.pl`](models/flag-lock-faulty-control.pl)

## Reproduction record

The new SMT files contain no `(check-sat)`, `(get-model)`, or solver-specific
epilogue. For each file, the exact tool invocation was:

```text
mcp__chiasmus.chiasmus_lint(
  solver="z3", input=<verbatim contents of the listed model file>)
mcp__chiasmus.chiasmus_verify(
  solver="z3", input=<verbatim contents of the listed model file>)
```

All 22 SMT files returned `fixes=[]` and `errors=[]` from `chiasmus_lint`.
`chiasmus_verify` returned:

```text
accept-inline-executor-stop-buggy.smt2 sat
accept-inline-executor-stop-corrected.smt2 unsat
accept-inline-executor-stop-nonvacuity.smt2 sat
accept-publication-stop-buggy.smt2      sat
accept-publication-stop-corrected.smt2  unsat
accept-publication-stop-nonvacuity.smt2 sat
eof-byte-visibility-nonvacuity.smt2      sat
eof-byte-visibility.smt2                 unsat
eof-raw-peer-closed-control.smt2         sat
handler-admission-buggy.smt2             sat
handler-admission-corrected.smt2         unsat
handler-admission-nonvacuity.smt2        sat
shutdown-drain-buggy.smt2                sat
shutdown-drain-corrected.smt2            unsat
shutdown-drain-nonvacuity.smt2           sat
shutdown-drain-write-error-corrected.smt2 unsat
shutdown-drain-write-error-edge.smt2     sat
shutdown-drain-write-error-nonvacuity.smt2 sat
stop-admission-inflight-buggy.smt2       sat
task-tracker-retention-buggy.smt2        sat
task-tracker-retention-corrected.smt2    unsat
task-tracker-retention-nonvacuity.smt2   sat
```

The runtime gate remains:

```sh
joltc -M:test
```

## Remaining semantic gaps

- `jolt.net` token, lease, and idempotent-close correctness is a dependency
  invariant, not re-proved by the TCP models.
- Handler admission is observed only at `submit-marked-handler!` return.
  Direct-executor gate re-entry is modelled, but accepted-task scheduling and
  termination for arbitrary executors remain separate assumptions.
- The task-tracker model bounds the active set at freeze to four and assumes
  every such task terminates. It proves the source retention/counting
  transitions, not executor fairness or unbounded completion time.
- The stop CAS closes the reactor-action gate. An action acquired before that
  CAS may perform its remaining internal dispatches afterward; cleanup still
  waits for the action to return.
- Shutdown models one current context and one finite Buffer. They summarize
  repeated partial writes and do not prove unbounded scheduler or peer
  fairness.
- The successful legacy-callback proof is conditional on full write success.
  The repaired error proof covers `write-completion` waiters; the separate SAT
  edge records why the intentionally success-only callback cannot release every
  possible legacy waiter.
- EOF abstracts bytes to counts and one older-view/late-arrival/terminal-view
  transition. Framing and byte values remain runtime/property-test concerns.
- The lock model is a source-derived call graph. It proves absence of the old
  recursive acquisition path, not fairness of the runtime's lock/atom
  implementations.
