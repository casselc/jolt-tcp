; Known-SAT control for executor invocation while accept-gate is held.
;
; Executor permits a direct implementation. If execute invokes the accept
; handler inline while admit-accepted! still owns accept-gate, a handler call to
; public stop spins in request-stop! and never reaches stop's bounded wait.
(declare-const context_published Bool)
(declare-const working_reserved Bool)
(declare-const direct_executor Bool)
(declare-const handler_calls_stop Bool)
(declare-const gate_released_before_submission Bool)
(declare-const executor_called_while_gate_held Bool)
(declare-const stop_gate_acquired Bool)
(declare-const bounded_wait_reached Bool)
(declare-const self_deadlock Bool)

(assert (! context_published :named published_context_fixture))
(assert (! working_reserved :named working_reservation_fixture))
(assert (! direct_executor :named direct_executor_fixture))
(assert (! handler_calls_stop :named inline_stop_fixture))

; Fault: registry publication and execute both occur in the gate body.
(assert (! (not gate_released_before_submission)
           :named submission_remains_inside_gate))
(assert (! (= executor_called_while_gate_held
              (and context_published
                   (not gate_released_before_submission)))
           :named executor_gate_ownership_definition))
(assert (! (= stop_gate_acquired
              (and direct_executor
                   context_published
                   handler_calls_stop
                   gate_released_before_submission))
           :named inline_stop_gate_acquisition_definition))
(assert (! (= bounded_wait_reached stop_gate_acquired)
           :named bounded_wait_requires_gate_acquisition))
(assert (! (= self_deadlock
              (and executor_called_while_gate_held
                   direct_executor
                   handler_calls_stop
                   (not stop_gate_acquired)
                   (not bounded_wait_reached)))
           :named self_deadlock_definition))

(assert (! self_deadlock :named self_deadlock_query))
