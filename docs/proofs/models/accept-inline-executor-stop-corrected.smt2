; Counterexample query for publish-reserve, gate release, then executor submit.
;
; WORKING is reserved before the Context becomes visible. execute runs only
; after accept-gate is released, so a legal direct executor may call the handler
; inline and that handler can acquire the stop gate and reach the bounded wait.
(declare-const context_published Bool)
(declare-const working_reserved_before_publication Bool)
(declare-const direct_executor Bool)
(declare-const handler_calls_stop Bool)
(declare-const external_stop_before_submission Bool)
(declare-const gate_released_before_submission Bool)
(declare-const handler_submission_attempted Bool)
(declare-const executor_called_while_gate_held Bool)
(declare-const inline_stop_gate_acquired Bool)
(declare-const bounded_wait_reached Bool)
(declare-const post_stop_submission_protected Bool)
(declare-const violation Bool)

(assert (! context_published :named published_context_fixture))
(assert (! direct_executor :named direct_executor_fixture))
(assert (! handler_calls_stop :named inline_stop_fixture))

(assert (! (= working_reserved_before_publication context_published)
           :named publication_reserves_working))
(assert (! gate_released_before_submission
           :named gate_release_precedes_submission))
(assert (! (= handler_submission_attempted context_published)
           :named published_context_is_submitted))
(assert (! (= executor_called_while_gate_held
              (and handler_submission_attempted
                   (not gate_released_before_submission)))
           :named executor_gate_ownership_definition))
(assert (! (= inline_stop_gate_acquired
              (and direct_executor
                   handler_submission_attempted
                   handler_calls_stop
                   gate_released_before_submission))
           :named inline_stop_gate_acquisition_definition))
(assert (! (= bounded_wait_reached inline_stop_gate_acquired)
           :named bounded_wait_reachability_definition))
(assert (! (= post_stop_submission_protected
              (or (not external_stop_before_submission)
                  working_reserved_before_publication))
           :named post_stop_working_reservation_definition))
(assert (! (= violation
              (or (and context_published
                       (not working_reserved_before_publication))
                  executor_called_while_gate_held
                  (and direct_executor
                       handler_submission_attempted
                       handler_calls_stop
                       (not inline_stop_gate_acquired))
                  (and direct_executor
                       handler_submission_attempted
                       handler_calls_stop
                       (not bounded_wait_reached))
                  (not post_stop_submission_protected)))
           :named violation_definition))

(assert (! violation :named violation_query))
