; NEGATIVE CONTROL for reactor-cursor-ordering-corrected.smt2. Task W6A.1.
;
; Exactly one assertion differs -- the reactor samples its wake cursor after the
; :pending drain rather than before it -- and the missed-publication park
; becomes reachable. jolt-net still honours the cursor faithfully here; the
; point of this control is that faithfulness is not enough, because the consumer
; chose a boundary that cannot describe its own read.
;
; The witness is the observed jolt-tcp interleaving: drain-pending! empties the
; set, a worker's finish-work! then marks its generation and wakes, and the
; cursor is only sampled afterwards, so that wake is at or below it and is
; consumed as stale. The reactor parks until the 1000 ms tick. Measured on real
; loopback before the fix: exchanges of 1091, 1098, 1121, and 2102 ms against a
; median of about 85 ms, the last being two ticks in one exchange.
;
; Expected: sat.

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
; THE FAULT, and the only difference from the corrected model. The cursor is
; sampled AFTER the drain -- which is what calling await-ready without a cursor
; amounts to, since await-ready then samples at its own entry. Everything else,
; including jolt-net's correct honouring of the cursor it is given, is
; unchanged: a sound poller cannot rescue a consumer that names the wrong
; boundary.
(assert
  (! (> t_cursor_sample t_drain_pending)
     :named reactor_samples_cursor_after_draining))
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

(assert (! reactor_parks_on_missed_work :named property_violated))

(check-sat)
(get-model)
