; Known-SAT control for handler admission.
;
; Fault injected: submit-handler! publishes WORKING, but a synchronous executor
; rejection neither requests close nor runs the task's finally.  The exact
; violation query is shared with handler-admission-corrected.smt2.
(declare-datatypes () ((SubmitOutcome admitted rejected)))
(declare-const submit_outcome SubmitOutcome)
(declare-const task_admitted Bool)
(declare-const task_finally_completed_before_return Bool)
(declare-const working_after_return Bool)
(declare-const rejection_close_requested Bool)
(declare-const violation Bool)

(assert (! (= submit_outcome rejected)
           :named synchronous_rejection_fixture))
(assert (! (= task_admitted (= submit_outcome admitted))
           :named executor_outcome_defines_admission))
(assert (! (not task_finally_completed_before_return)
           :named rejected_task_has_no_finally))

; Faulty implementation: WORKING was set before execute and only the task's
; finally could clear it.
(assert (! (= working_after_return
              (not task_finally_completed_before_return))
           :named faulty_working_clear_only_in_task))
(assert (! (= rejection_close_requested false)
           :named faulty_rejection_omits_close_request))

(assert (! (= violation
              (or (and working_after_return
                       (not task_admitted))
                  (and (= submit_outcome rejected)
                       (or working_after_return
                           (not rejection_close_requested)))))
           :named violation_definition))
(assert (! violation :named violation_query))
