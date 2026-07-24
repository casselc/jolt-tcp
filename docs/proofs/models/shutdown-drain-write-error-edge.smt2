; Deliberate SAT limitation witness for the success-only callback contract.
;
; This is not the known-buggy control.  It uses the corrected retirement order
; but changes the native write outcome to an error.  handle-write cannot complete
; the Buffer, so its success callback is ineligible; the error requests close,
; but a handler blocked only on that callback can remain WORKING and therefore
; cannot be retired by the safe shutdown drain.
(declare-datatypes () ((WriteOutcome full_success write_error)))
(declare-const write_outcome WriteOutcome)
(declare-const working_at_stop Bool)
(declare-const writing_at_stop Bool)
(declare-const current_context Bool)
(declare-const closed_before_drain Bool)
(declare-const retired_before_quiescence Bool)
(declare-const write_interest_during_drain Bool)
(declare-const write_ready_within_bound Bool)
(declare-const write_serviced_within_bound Bool)
(declare-const full_write_completed Bool)
(declare-const close_requested_after_service Bool)
(declare-const callback_eligible Bool)
(declare-const callback_runs_within_bound Bool)
(declare-const handler_waits_for_success_callback Bool)
(declare-const handler_completes_within_bound Bool)
(declare-const working_at_bound Bool)
(declare-const retired_at_bound Bool)
(declare-const error_edge_exposed Bool)

(assert (! working_at_stop :named active_handler_fixture))
(assert (! writing_at_stop :named queued_write_fixture))
(assert (! current_context :named current_context_fixture))
(assert (! (not closed_before_drain) :named open_context_fixture))
(assert (! handler_waits_for_success_callback :named callback_wait_fixture))
(assert (! (= write_outcome write_error)
           :named native_write_error_fixture))
(assert (! (= retired_before_quiescence false)
           :named retirement_follows_handler_quiescence))
(assert (! (= write_interest_during_drain
              (and current_context (not closed_before_drain)
                   (not retired_before_quiescence)
                   working_at_stop writing_at_stop))
           :named write_only_interest_definition))

; Readiness and reactor fairness get the failing write as far as handle-write.
(assert (! (= write_ready_within_bound write_interest_during_drain)
           :named fair_write_readiness))
(assert (! (= write_serviced_within_bound write_ready_within_bound)
           :named fair_reactor_service))
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
(assert (! (= callback_runs_within_bound callback_eligible)
           :named fair_callbacks_cannot_run_ineligible_callback))
(assert (! (= handler_completes_within_bound
              (and working_at_stop
                   (or (not handler_waits_for_success_callback)
                       callback_runs_within_bound)))
           :named handler_wait_definition))
(assert (! (= working_at_bound
              (and working_at_stop
                   (not handler_completes_within_bound)))
           :named working_at_bound_definition))
(assert (! (= retired_at_bound
              (and current_context
                   (not retired_before_quiescence)
                   (not working_at_bound)))
           :named final_retirement_definition))
(assert (! (= error_edge_exposed
              (and close_requested_after_service
                   (not callback_eligible)
                   working_at_bound
                   (not retired_at_bound)))
           :named error_edge_definition))
(assert (! error_edge_exposed :named error_edge_query))
