; Non-vacuity control for the corrected shutdown drain.
;
; Read and listener readiness are present but gated off.  The active handler's
; finite write reaches readiness/service, its success callback runs, WORKING
; clears, and only then is the current Context retired.
; accept_admitted_after_stop/read_admitted_after_stop mean reactor-action
; acquisition after the stop CAS, not internal dispatch by a pre-acquired action.
(declare-datatypes () ((WriteOutcome full_success write_error)))
(declare-datatypes () ((AdmissionPhase open stopping)))
(declare-const write_outcome WriteOutcome)

(declare-const stop_requested Bool)
(declare-const working_at_stop Bool)
(declare-const writing_at_stop Bool)
(declare-const current_context Bool)
(declare-const closed_before_drain Bool)
(declare-const handler_waits_for_success_callback Bool)
(declare-const reactor_batch_in_flight_at_stop Bool)
(declare-const ordinary_event_admitted_before_stop Bool)
(declare-const ordinary_event_finished_before_stop_cas Bool)
(declare-const ordinary_event_active_after_stop_cas Bool)
(declare-const ordinary_event_completed_before_cleanup Bool)
(declare-const active_admissions_at_cleanup Int)
(declare-const admission_phase_after_stop AdmissionPhase)
(declare-const admission_phase_after_release AdmissionPhase)
(declare-const listener_ready_after_stop Bool)
(declare-const socket_read_ready_after_stop Bool)
(declare-const running_after_stop Bool)
(declare-const listener_registered_before_cleanup Bool)
(declare-const read_interest_before_cleanup Bool)
(declare-const listener_registered_during_drain Bool)
(declare-const read_interest_during_drain Bool)
(declare-const accept_admitted_after_stop Bool)
(declare-const read_admitted_after_stop Bool)
(declare-const retired_before_quiescence Bool)
(declare-const write_interest_during_drain Bool)
(declare-const write_ready_within_bound Bool)
(declare-const write_serviced_within_bound Bool)
(declare-const full_write_completed Bool)
(declare-const callback_eligible Bool)
(declare-const callback_runs_within_bound Bool)
(declare-const handler_completes_within_bound Bool)
(declare-const working_at_bound Bool)
(declare-const retired_at_bound Bool)

(assert (! stop_requested :named stop_fixture))
(assert (! working_at_stop :named active_handler_fixture))
(assert (! writing_at_stop :named queued_write_fixture))
(assert (! current_context :named current_context_fixture))
(assert (! (not closed_before_drain) :named open_context_fixture))
(assert (! handler_waits_for_success_callback :named callback_wait_fixture))
(assert (! reactor_batch_in_flight_at_stop
           :named in_flight_native_batch_fixture))
(assert (! ordinary_event_admitted_before_stop
           :named pre_stop_admitted_event_fixture))
(assert (! (not ordinary_event_finished_before_stop_cas)
           :named pre_stop_event_remains_active_across_stop_cas))
(assert (! listener_ready_after_stop :named listener_ready_fixture))
(assert (! socket_read_ready_after_stop :named read_ready_fixture))
(assert (! listener_registered_before_cleanup
           :named listener_was_registered_at_stop))
(assert (! read_interest_before_cleanup
           :named read_was_registered_at_stop))
(assert (! (= write_outcome full_success)
           :named finite_write_succeeds_within_bound))

(assert (! (= admission_phase_after_stop stopping)
           :named stop_cas_closes_admission))
(assert (! (= running_after_stop
              (not stop_requested))
           :named stop_clears_running))
(assert (! (= ordinary_event_active_after_stop_cas
              (and ordinary_event_admitted_before_stop
                   (not ordinary_event_finished_before_stop_cas)))
           :named pre_stop_action_may_cross_stop_cas))
(assert (! (= ordinary_event_completed_before_cleanup
              ordinary_event_admitted_before_stop)
           :named single_reactor_completes_pre_admitted_before_cleanup))
(assert (! (and (<= 0 active_admissions_at_cleanup)
                (<= active_admissions_at_cleanup 1))
           :named active_admission_bound))
(assert (! (= active_admissions_at_cleanup
              (ite (and ordinary_event_admitted_before_stop
                        (not ordinary_event_completed_before_cleanup))
                   1 0))
           :named active_admissions_at_cleanup_definition))
(assert (! (= admission_phase_after_release admission_phase_after_stop)
           :named release_preserves_stopping_phase))
(assert (! (= accept_admitted_after_stop
              (and stop_requested reactor_batch_in_flight_at_stop
                   (= admission_phase_after_stop open)
                   listener_ready_after_stop
                   listener_registered_before_cleanup))
           :named accept_requires_open_admission_cas))
(assert (! (= read_admitted_after_stop
              (and stop_requested reactor_batch_in_flight_at_stop
                   (= admission_phase_after_stop open)
                   socket_read_ready_after_stop
                   read_interest_before_cleanup))
           :named read_requires_open_admission_cas))
(assert (! (= listener_registered_during_drain
              (not stop_requested))
           :named stop_retires_listener))
(assert (! (= read_interest_during_drain false)
           :named stop_revokes_read_interest))
(assert (! (= retired_before_quiescence false)
           :named retirement_follows_handler_quiescence))
(assert (! (= write_interest_during_drain
              (and stop_requested current_context
                   (not closed_before_drain)
                   (not retired_before_quiescence)
                   working_at_stop writing_at_stop))
           :named write_only_interest_definition))

; Fairness assumptions are fixed true in this witness.
(assert (! (= write_ready_within_bound write_interest_during_drain)
           :named fair_write_readiness))
(assert (! (= write_serviced_within_bound write_ready_within_bound)
           :named fair_reactor_service))
(assert (! (= full_write_completed
              (and write_serviced_within_bound
                   (= write_outcome full_success)))
           :named full_write_success_definition))
(assert (! (= callback_eligible full_write_completed)
           :named success_only_callback_definition))
(assert (! (= callback_runs_within_bound callback_eligible)
           :named fair_callback_execution))
(assert (! (= handler_completes_within_bound
              (and working_at_stop callback_runs_within_bound))
           :named fair_handler_termination))
(assert (! (= working_at_bound
              (and working_at_stop
                   (not handler_completes_within_bound)))
           :named working_at_bound_definition))
(assert (! (= retired_at_bound
              (and current_context
                   (not retired_before_quiescence)
                   (not working_at_bound)))
           :named final_retirement_definition))

(assert (! (and (not accept_admitted_after_stop)
                (not read_admitted_after_stop)
                ordinary_event_active_after_stop_cas
                ordinary_event_completed_before_cleanup
                (= active_admissions_at_cleanup 0)
                (= admission_phase_after_release stopping)
                write_interest_during_drain
                write_serviced_within_bound
                callback_runs_within_bound
                (not working_at_bound)
                retired_at_bound)
           :named complete_shutdown_trace_reachable))
