; Corrected bounded write-error progress query for a failure-aware completion.
;
; Milestones: the reactor services one queued write, native send fails, write
; admission closes, every already-admitted producer leaves, the stable queue is
; drained, its outcome promise is settled with failure, the waiting handler
; clears WORKING, CLOSED readiness is cleared, and only then may retirement run.
(declare-datatypes () ((WriteOutcome full_success write_error)))
(declare-datatypes () ((WriteAdmission open closed)))
(declare-const write_outcome WriteOutcome)
(declare-const write_admission_after_failure WriteAdmission)

(declare-const working_at_stop Bool)
(declare-const writing_at_stop Bool)
(declare-const current_context Bool)
(declare-const handler_waits_for_outcome Bool)
(declare-const reactor_service_fair Bool)
(declare-const handler_termination_fair Bool)

(declare-const write_serviced_within_bound Bool)
(declare-const close_requested Bool)
(declare-const active_write_producers_at_drain Int)
(declare-const stable_queue_drain Bool)
(declare-const outcome_failure_eligible Bool)
(declare-const outcome_settled Bool)
(declare-const handler_released Bool)
(declare-const handler_completes Bool)
(declare-const working_at_bound Bool)
(declare-const closed_at_bound Bool)
(declare-const write_interest_at_bound Bool)
(declare-const retired_at_bound Bool)
(declare-const outcome_step Int)
(declare-const retirement_step Int)
(declare-const outcome_before_retirement Bool)
(declare-const progress_assumptions Bool)
(declare-const progress_stalled Bool)

(assert (! working_at_stop :named active_handler_fixture))
(assert (! writing_at_stop :named queued_write_fixture))
(assert (! current_context :named current_context_fixture))
(assert (! handler_waits_for_outcome :named outcome_wait_fixture))
(assert (! (= write_outcome write_error) :named native_write_error_fixture))
(assert (! reactor_service_fair :named reactor_services_failing_write))
(assert (! handler_termination_fair :named released_handler_terminates))

(assert (! (= write_serviced_within_bound
              (and working_at_stop writing_at_stop reactor_service_fair))
           :named bounded_write_service_definition))
(assert (! (= close_requested
              (and write_serviced_within_bound
                   (= write_outcome write_error)))
           :named write_error_requests_close))
(assert (! (= write_admission_after_failure
              (ite close_requested closed open))
           :named failure_closes_write_admission))

; close-write-admission! does not return until the pre-close active count is
; zero. Closed admission rejects every later producer, making empty stable.
(assert (! (and (<= 0 active_write_producers_at_drain)
                (<= active_write_producers_at_drain 1))
           :named active_write_producer_bound))
(assert (! (= active_write_producers_at_drain
              (ite (= write_admission_after_failure closed) 0 1))
           :named closed_admission_barrier_definition))
(assert (! (= stable_queue_drain
              (and (= write_admission_after_failure closed)
                   (= active_write_producers_at_drain 0)))
           :named stable_queue_drain_definition))

(assert (! (= outcome_failure_eligible
              (and stable_queue_drain write_serviced_within_bound
                   (= write_outcome write_error)))
           :named failure_aware_outcome_definition))
(assert (! (= outcome_settled outcome_failure_eligible)
           :named failure_outcome_settled_before_retirement))
(assert (! (= handler_released
              (and handler_waits_for_outcome outcome_settled))
           :named outcome_releases_waiting_handler))
(assert (! (= handler_completes
              (and working_at_stop handler_released
                   handler_termination_fair))
           :named handler_completion_definition))
(assert (! (= working_at_bound
              (and working_at_stop (not handler_completes)))
           :named working_at_bound_definition))
(assert (! (= closed_at_bound close_requested)
           :named closed_state_definition))
(assert (! (= write_interest_at_bound
              (and current_context writing_at_stop
                   (not closed_at_bound)))
           :named closed_context_clears_readiness))
(assert (! (= retired_at_bound
              (and current_context (not working_at_bound)))
           :named retirement_after_handler_quiescence))

; The finite milestone order records the implementation sequence without
; pretending to prove unbounded scheduler time.
(assert (! (= outcome_step (ite outcome_settled 2 4))
           :named outcome_milestone))
(assert (! (= retirement_step (ite retired_at_bound 3 1))
           :named retirement_milestone))
(assert (! (= outcome_before_retirement
              (and outcome_settled retired_at_bound
                   (< outcome_step retirement_step)))
           :named completion_precedes_retirement))

(assert (! (= progress_assumptions
              (and working_at_stop writing_at_stop current_context
                   handler_waits_for_outcome
                   (= write_outcome write_error)
                   reactor_service_fair handler_termination_fair))
           :named progress_assumptions_definition))
(assert (! (= progress_stalled
              (and progress_assumptions
                   (or (not stable_queue_drain)
                       (not outcome_settled)
                       working_at_bound
                       write_interest_at_bound
                       (not retired_at_bound)
                       (not outcome_before_retirement))))
           :named progress_stalled_definition))

(assert (! progress_stalled :named progress_stalled_query))
