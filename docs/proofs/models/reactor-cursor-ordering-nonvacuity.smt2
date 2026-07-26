; Claim: the reactor samples its wake cursor early enough that no worker
; publication can be both missed by process-pending! and discarded before the
; native wait. Task W6A.1.
;
; This is the CONSUMER half of the contract. jolt-net's
; wake-cursor-ordering-corrected.smt2 proves the other half -- that await-ready
; honours whatever cursor it is handed. Neither is sufficient alone: a poller
; that honours the cursor still parks if the reactor samples it at the wrong
; moment, and the wrong moment is anywhere at or after the drain.
;
; The obligation is therefore entirely about ORDER, and the fault it guards
; against is the natural way to write the loop:
;
;   sample the cursor        <- correct, and what reactor-loop now does
;   process-pending!            (the reactor's read of worker-owned state)
;   await-ready with cursor
;
; versus sampling inside or after the drain, which reintroduces exactly the
; window the cursor exists to close.
;
; Times are integers on a total order; no duration is used anywhere. The 1000 ms
; native safety tick sets what a lost wake COSTS, not whether one happens.
;
; Domain: one worker publication (finish-work!: mark-pending! then
; wake-server!), one reactor turn.
; Expected: sat -- the corrected reactor still permits a genuine park.

(declare-const t_mark_pending Int)
(declare-const t_wake Int)
(declare-const t_cursor_sample Int)
(declare-const t_drain_pending Int)
(declare-const t_await Int)

(declare-const drain_observed_publication Bool)
(declare-const wake_is_above_cursor Bool)
(declare-const wait_is_armed Bool)
(declare-const reactor_parks_on_missed_work Bool)

(assert
  (! (distinct t_mark_pending t_wake t_cursor_sample t_drain_pending t_await)
     :named events_are_distinct_instants))

; --- producer: teensyp.server/finish-work! -----------------------------------
; unset-flag! WORKING, mark-pending!, then wake-server!. The generation is in
; the :pending set before the wake that announces it.
(assert
  (! (< t_mark_pending t_wake) :named worker_marks_pending_before_waking))

; --- consumer: teensyp.server/reactor-loop -----------------------------------
; THE FIX, and the only assertion reactor-cursor-ordering-buggy.smt2 changes.
(assert
  (! (< t_cursor_sample t_drain_pending)
     :named reactor_samples_cursor_before_draining))
(assert
  (! (< t_drain_pending t_await) :named reactor_drains_before_awaiting))

; drain-pending! empties the set atomically, so it sees exactly the generations
; marked before it ran.
(assert
  (! (= drain_observed_publication (< t_mark_pending t_drain_pending))
     :named drain_sees_exactly_what_was_marked_before_it))

; jolt.net refuses to discard a wake whose sequence exceeds the supplied cursor.
(assert
  (! (= wake_is_above_cursor (> t_wake t_cursor_sample))
     :named wake_is_fresh_iff_it_follows_the_sample))
(assert
  (! (= wait_is_armed wake_is_above_cursor)
     :named jolt_net_arms_the_wait_for_a_fresh_wake))

; --- the violation -----------------------------------------------------------
; The reactor holds work it has not processed and nothing will wake it before
; the safety tick. An observed publication is fine (it is being processed now),
; and an armed wait is fine (it returns at once).
(assert
  (! (= reactor_parks_on_missed_work
        (and (not drain_observed_publication) (not wait_is_armed)))
     :named violation_iff_unseen_work_parks_unarmed))

; NON-VACUITY. The corrected model must not hold merely by forbidding every
; execution, and the fix must not degenerate into "always arm", which would keep
; the query unsat while turning every idle reactor turn into a spin. So ask for
; the execution the reactor must KEEP: the drain did observe the publication and
; the wait parks unarmed. Parking is correct there -- the work is already in
; hand and the reactor has nothing outstanding to be woken for.
;
; This is the model-level twin of the "pre-cursor publication" control in
; jolt-net's test/jolt/net/wake_cursor_test.clj.
;
; Expected: sat.
(assert
  (! (not reactor_parks_on_missed_work) :named property_holds))
(assert
  (! drain_observed_publication :named the_drain_did_observe_the_work))
(assert (! (not wait_is_armed) :named and_the_wait_still_parks))

(check-sat)
(get-model)
