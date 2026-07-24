; Bounded handler-admission counterexample query.
;
; Observation boundary: immediately after submit-handler! returns.
; The admitted task may already have completed its finally, or may still own
; WORKING.  A synchronously rejected task is never admitted and therefore has
; no task finally; the catch path requests close and clears WORKING itself.
(declare-datatypes () ((SubmitOutcome admitted rejected)))
(declare-const submit_outcome SubmitOutcome)
(declare-const task_admitted Bool)
(declare-const task_finally_completed_before_return Bool)
(declare-const working_after_return Bool)
(declare-const rejection_close_requested Bool)
(declare-const violation Bool)

(assert (! (= task_admitted (= submit_outcome admitted))
           :named executor_outcome_defines_admission))
(assert (! (not (and task_finally_completed_before_return
                     (not task_admitted)))
           :named only_admitted_task_can_run_finally))
(assert (! (= working_after_return
              (and task_admitted
                   (not task_finally_completed_before_return)))
           :named admitted_task_or_rejection_clears_working))
(assert (! (= rejection_close_requested
              (= submit_outcome rejected))
           :named synchronous_rejection_requests_close))

(assert (! (= violation
              (or (and working_after_return
                       (not task_admitted))
                  (and (= submit_outcome rejected)
                       (or working_after_return
                           (not rejection_close_requested)))))
           :named violation_definition))
(assert (! violation :named violation_query))
