; Supplemental known-SAT control for the monotonic admission CAS.
;
; An await-ready batch exists while admission is :open.  Stop changes the phase
; to :stopping.  A faulty split check/increment then uses its stale :open
; observation to acquire accept/read reactor actions after the stop CAS.  The
; corrected code CAS-increments :active only if the *current* phase is still
; :open, so exactly one of the action-acquire CAS and stop CAS can win first.
; "Admitted" variable names below refer to that action acquisition, not to every
; side effect of an action that acquired before stop.
(declare-datatypes () ((AdmissionPhase open stopping)))
(declare-const admission_phase_after_stop AdmissionPhase)
(declare-const candidate_observed_open_before_stop Bool)
(declare-const reactor_batch_in_flight_at_stop Bool)
(declare-const listener_ready_after_stop Bool)
(declare-const socket_read_ready_after_stop Bool)
(declare-const listener_registered_before_cleanup Bool)
(declare-const read_interest_before_cleanup Bool)
(declare-const accept_admitted_after_stop Bool)
(declare-const read_admitted_after_stop Bool)
(declare-const violation Bool)

(assert (! (= admission_phase_after_stop stopping)
           :named stop_cas_closes_admission))
(assert (! candidate_observed_open_before_stop
           :named stale_open_observation_fixture))
(assert (! reactor_batch_in_flight_at_stop
           :named in_flight_native_batch_fixture))
(assert (! listener_ready_after_stop :named listener_ready_fixture))
(assert (! socket_read_ready_after_stop :named read_ready_fixture))
(assert (! listener_registered_before_cleanup
           :named listener_was_registered_at_stop))
(assert (! read_interest_before_cleanup
           :named read_was_registered_at_stop))

; Fault: admission uses the stale observation instead of one CAS over the
; current {:phase :active} value.
(assert (! (= accept_admitted_after_stop
              (and reactor_batch_in_flight_at_stop
                   candidate_observed_open_before_stop
                   listener_ready_after_stop
                   listener_registered_before_cleanup))
           :named faulty_accept_uses_stale_open_observation))
(assert (! (= read_admitted_after_stop
              (and reactor_batch_in_flight_at_stop
                   candidate_observed_open_before_stop
                   socket_read_ready_after_stop
                   read_interest_before_cleanup))
           :named faulty_read_uses_stale_open_observation))
(assert (! (= violation
              (or accept_admitted_after_stop
                  read_admitted_after_stop))
           :named violation_definition))
(assert (! violation :named violation_query))
