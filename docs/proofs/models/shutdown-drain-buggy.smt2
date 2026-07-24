; Known-SAT control for shutdown drain.
;
; Fault injected: cleanup retires a current Context immediately after stop,
; before its active handler's queued write can be serviced.  The same
; violation predicate is used by shutdown-drain-corrected.smt2.
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

(declare-const peer_drains Bool)
(declare-const kernel_write_ready_fair Bool)
(declare-const reactor_service_fair Bool)
(declare-const callback_executor_fair Bool)
(declare-const handler_termination_fair Bool)

(declare-const retired_before_quiescence Bool)
(declare-const write_interest_during_drain Bool)
(declare-const write_ready_within_bound Bool)
(declare-const write_serviced_within_bound Bool)
(declare-const full_write_completed Bool)
(declare-const close_requested_after_service Bool)
(declare-const callback_eligible Bool)
(declare-const callback_runs_within_bound Bool)
(declare-const handler_released_within_bound Bool)
(declare-const handler_completes_within_bound Bool)
(declare-const working_at_bound Bool)
(declare-const retired_at_bound Bool)
(declare-const retired_while_working Bool)
(declare-const progress_assumptions Bool)
(declare-const progress_stalled Bool)
(declare-const violation Bool)

; One active, current, not-yet-closed handler waits for a finite queued Buffer's
; success callback.  Hostile read/accept readiness remains present after stop.
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

; Explicit bounded progress assumptions for the success path.
(assert (! peer_drains :named peer_eventually_drains))
(assert (! kernel_write_ready_fair
           :named kernel_eventually_reports_write_ready))
(assert (! reactor_service_fair
           :named reactor_eventually_services_write_ready))
(assert (! callback_executor_fair
           :named callback_executor_eventually_runs_accepted_callback))
(assert (! handler_termination_fair
           :named released_handler_eventually_terminates))
(assert (! (= write_outcome full_success)
           :named finite_write_succeeds_within_bound))

; The monotonic admission CAS and listener/read revocation are correct in this
; control.  Only the early Context retirement below is faulty.
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

; Fault: socket retirement happens before the handler quiesces.
(assert (! (= retired_before_quiescence stop_requested)
           :named faulty_immediate_retirement))
(assert (! (= write_interest_during_drain
              (and stop_requested current_context
                   (not closed_before_drain)
                   (not retired_before_quiescence)
                   working_at_stop writing_at_stop))
           :named write_only_interest_definition))
(assert (! (= write_ready_within_bound
              (and write_interest_during_drain peer_drains
                   kernel_write_ready_fair))
           :named bounded_write_readiness_definition))
(assert (! (= write_serviced_within_bound
              (and write_ready_within_bound reactor_service_fair))
           :named bounded_write_service_definition))
(assert (! (= full_write_completed
              (and write_serviced_within_bound
                   (= write_outcome full_success)))
           :named full_write_success_definition))
(assert (! (= close_requested_after_service
              (and write_serviced_within_bound
                   (= write_outcome write_error)))
           :named write_error_requests_close_definition))
(assert (! (= callback_eligible full_write_completed)
           :named success_only_callback_definition))
(assert (! (= callback_runs_within_bound
              (and callback_eligible callback_executor_fair))
           :named callback_fairness_definition))
(assert (! (= handler_released_within_bound
              (or (not handler_waits_for_success_callback)
                  callback_runs_within_bound))
           :named handler_release_definition))
(assert (! (= handler_completes_within_bound
              (and working_at_stop handler_released_within_bound
                   handler_termination_fair))
           :named handler_termination_definition))
(assert (! (= working_at_bound
              (and working_at_stop
                   (not handler_completes_within_bound)))
           :named working_at_bound_definition))
(assert (! (= retired_at_bound
              (and current_context
                   (not retired_before_quiescence)
                   (not working_at_bound)))
           :named final_retirement_definition))
(assert (! (= retired_while_working
              (or (and retired_before_quiescence working_at_stop)
                  (and retired_at_bound working_at_bound)))
           :named retirement_violation_definition))
(assert (! (= progress_assumptions
              (and stop_requested working_at_stop writing_at_stop
                   current_context (not closed_before_drain)
                   handler_waits_for_success_callback
                   peer_drains kernel_write_ready_fair
                   reactor_service_fair callback_executor_fair
                   handler_termination_fair
                   (= write_outcome full_success)))
           :named progress_assumptions_definition))
(assert (! (= progress_stalled
              (and progress_assumptions
                   (or working_at_bound
                       (not write_serviced_within_bound)
                       (not callback_runs_within_bound)
                       (not retired_at_bound))))
           :named progress_stalled_definition))
(assert (! (= violation
              (or accept_admitted_after_stop
                  read_admitted_after_stop
                  listener_registered_during_drain
                  read_interest_during_drain
                  (> active_admissions_at_cleanup 0)
                  (not (= admission_phase_after_release stopping))
                  retired_while_working
                  progress_stalled))
           :named violation_definition))
(assert (! violation :named violation_query))
