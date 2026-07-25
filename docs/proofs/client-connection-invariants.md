# Bounded outbound-client invariants

Checked on 2026-07-24 with Chiasmus/Z3 and the cache-off Jolt runtime suite.

These models cover at most three resolver candidates. They prove properties of
the connector state machine extracted from `teensyp.client`, not DNS
cancellation, scheduler fairness, POSIX itself, or the lower-level
`jolt.net` implementation. `jolt.net` owns descriptor generations, close/lease
coordination, poller wakeup, and native error capture; its own proof record
covers those obligations.

## 1. One absolute deadline spans every candidate

### Bounded claim

For one to three resolver candidates, every readiness wait is recomputed from
the same absolute monotonic deadline. Time consumed by a failed candidate
therefore reduces the next candidate's wait; candidate advancement never
restarts the caller's relative timeout.

This claim is conditional on a finite deadline. An explicit
`:connect-timeout-ms nil` selects unbounded connect policy and therefore has no
deadline to preserve or connect-liveness bound to prove. Candidate/socket/poller
ownership and cleanup obligations still apply in that mode.

The negated query asks whether any wait differs from the remaining budget or
whether bounded candidate work that consumes no more than each remaining budget
can finish after the original deadline. The arithmetic model uses integral
milliseconds. Production uses monotonic nanoseconds and a ceiling conversion to
milliseconds, then rechecks the nanosecond deadline after every wait. The model
therefore excludes the sub-millisecond rounding/scheduler overshoot that the
runtime detects as timeout rather than success.

### Source facts

- `src/teensyp/client.clj:90-97` computes a finite wait from
  `(deadline - now)`; there is no relative timeout argument in the candidate
  loop.
- `src/teensyp/client.clj:252-266` takes one lexical `deadline` into the whole
  resolver-order loop and checks it before every candidate.
- `src/teensyp/client.clj:306-322` recomputes every poll wait from that same
  value and resolves readiness through `jolt.net/finish-connect!`.
- `src/teensyp/client.clj:324-356` carries only `more` and `last-ex` when it
  advances. The deadline is neither replaced nor incremented.
- `src/teensyp/client.clj:640-678` creates the deadline before resolution,
  removes deadline policy from the socket options, resolves once, and passes
  the one absolute value into the state machine.

### Checked models

| Model | Expected | Chiasmus/Z3 result | Evidence |
|---|---:|---:|---|
| [`client-deadline-buggy.smt2`](models/client-deadline-buggy.smt2) | SAT | **SAT** | the control resets candidate 2 to 10 ms after candidate 1 consumed 6 ms; `finish=12`, `deadline=10`, `violation=true` |
| [`client-deadline-corrected.smt2`](models/client-deadline-corrected.smt2) | UNSAT | **UNSAT** | the core includes all three same-deadline wait equations, bounded durations, finish time, violation definition, and query |
| [`client-deadline-nonvacuity.smt2`](models/client-deadline-nonvacuity.smt2) | SAT | **SAT** | candidate 1 consumes 4/10 ms, candidate 2 receives 6 ms and succeeds at time 6; candidate 3 is not attempted |

All three models passed `chiasmus_lint` with no fixes or errors before
verification.

The executable semantic oracle is
`test/teensyp/client_test.clj:296-524`. Its injected monotonic clock makes the
two observed poll budgets exactly `[10 6]`, distinguishes
`::connect-timeout`, retains the previous real native connect exception as the
timeout's cause, and requires exhaustion to throw the final real candidate
exception unchanged. Resolver and completion failures observed at the deadline
remain causes of a structurally distinct timeout. A synchronous address-family
setup failure still advances resolver order, while a poller failure is not
misclassified as evidence against the current address.

The resolver call itself is synchronous in this slice. A deadline can be
checked before and after it, but cannot interrupt an in-flight
`getaddrinfo(3)`; a resolver backend with cancellation remains a lower-layer
follow-on.

## 2. Failure retains no candidate or connector-poller ownership

### Bounded claim

Across at most three candidates:

1. every attempted socket that is not selected is closed before advancement or
   terminal failure;
2. exhaustion or timeout closes the connector poller;
3. success transfers exactly the selected socket and that poller; and
4. failure while constructing the second, read-direction poller closes the
   already-transferred socket and write-direction poller.

The asserted violation is an open failed/unattempted socket, an open or
transferred poller after terminal failure, or a success without the selected
socket and poller both transferred.

### Source facts

- `src/teensyp/client.clj:236-244` removes a current registration and closes its
  socket; cleanup errors cannot replace the already-captured connect cause.
- `src/teensyp/client.clj:270-304` mirrors `jolt.net` by advancing every
  synchronously rolled-back address attempt, and retires sockets on deadline,
  registration, and immediate-completion failure.
- `src/teensyp/client.clj:324-365` retires an asynchronous failed attempt before
  advancing only native connect errors; poller/lifecycle failures fail fast,
  and unknown completion/initiation states are rejected fail-closed.
- `src/teensyp/client.clj:258-262` and `366-370` keep the connector poller under
  a `finally`; only a successful result flips the transfer flag.
- `src/teensyp/client.clj:504-638` creates and registers one persistent read
  poller. Every construction failure closes any read poller, the transferred
  write poller, and the socket.
- `src/teensyp/client.clj:482-502` makes public close idempotent, wakes blocked
  directions by closing their pollers first, always attempts all three closes,
  and retains the first cleanup exception.

### Checked models

| Model | Expected | Chiasmus/Z3 result | Evidence |
|---|---:|---:|---|
| [`client-ownership-buggy.smt2`](models/client-ownership-buggy.smt2) | SAT | **SAT** | exhausted outcome retains failed socket 1 and the poller; `violation=true` |
| [`client-ownership-corrected.smt2`](models/client-ownership-corrected.smt2) | UNSAT | **UNSAT** | the core includes bounded outcome, attempted-candidate derivation, all three socket ownership equations, poller ownership/transfer, and the violation query |
| [`client-ownership-nonvacuity.smt2`](models/client-ownership-nonvacuity.smt2) | SAT | **SAT** | candidate 2 succeeds; candidate 1 is closed, candidate 3 unattempted, and exactly socket 2 plus the poller remain open/transferred |

All three models passed `chiasmus_lint` with no fixes or errors before
verification.

The same deterministic tests require the current failed socket and poller to be
closed on timeout/exhaustion, while the selected socket and poller remain
transferred on success. The construction rollback witnesses at
`test/teensyp/client_test.clj:526-553` inject failure both before the read poller
can be created and after it is registered. They require every acquired resource
to close, including the already-transferred write poller and socket.

## 3. Blocking byte and concurrency semantics

These are executable contracts rather than separate solver models:

- `src/teensyp/client.clj:394-467` bounds readiness waits by the remaining
  absolute budget, retries `::would-block`, advances only by the positive count
  actually transferred, treats `::eof` as nil, and rejects an impossible
  transport result instead of spinning.
- `src/teensyp/client.clj:105-134` converts relative `:timeout-ms` to one
  absolute monotonic value and rejects combining it with `:deadline-nanos`.
  `src/teensyp/client.clj:695-746` exposes these additive options arities while
  keeping omission unbounded.
- `src/teensyp/client.clj:136-218` serializes same-direction operations through
  a FIFO promise-ticket gate rather than a thread-owned reentrant lock. Deadline
  acquisition marks an expired ticket cancelled; release atomically skips it,
  so queue time is included without transferring ownership after timeout.
- `src/teensyp/client.clj:518-597` gives observed EOF, reset, and close their
  transport semantics instead of translating them to timeout. A readiness event
  at the boundary earns exactly one non-blocking probe; partial progress with
  bytes remaining must recheck the original deadline before another retry.
- Read and write use distinct gates and persistent pollers, so one reader and
  one writer may block concurrently without per-operation poller allocation.
- `test/teensyp/client_test.clj:14-227` deterministically checks partial slices,
  already-expired deadlines, exact ceiling-derived wait budgets, would-block
  timeout, partial-progress timeout, pre-deadline success, and EOF/reset winning
  a simultaneous readiness/deadline race.
- `test/teensyp/client_test.clj:229-272` checks that cancelled tickets cannot
  steal or wedge gate ownership and retains ordinary FIFO exclusion.
- `test/teensyp/client_test.clj:566-642` drives the public API over real
  loopback TCP: expired and successful options arities, full-duplex send/read,
  endpoint inspection without a descriptor, idempotent half-close, echoed
  bytes, nil EOF, close winning an expired-deadline race, and idempotent full
  close.

The proof boundary does not claim operation liveness if a peer remains open but
never becomes ready and the caller selects the default unbounded policy. A
finite operation deadline bounds both gate acquisition and poll/retry work;
closing the connection remains independent cancellation and wakes its pollers.
Likewise, the connect-deadline theorem does not claim liveness when the caller
explicitly selects `:connect-timeout-ms nil`; the deterministic
`explicit-unbounded-connect-does-not-inherit-the-default-deadline` regression
checks that this mode remains unbounded without weakening ownership cleanup.
The operation rules above are executable contracts rather than an additional
solver model. Windows socket runtime remains unavailable until `jolt.net`
supplies its non-blocking completion backend. Windows source-mode CI can still
exercise the descriptor-independent buffer model; that is portable logic
evidence, not evidence for these connection invariants.
