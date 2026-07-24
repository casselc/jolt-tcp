; Non-vacuity control for both real submit-handler! outcomes.
;
; One admitted task remains WORKING at the observation boundary.  A separate
; rejected task is recovered synchronously: it is not WORKING and close was
; requested.  Both scenarios use the corrected equations.
(declare-const admitted_task_admitted Bool)
(declare-const admitted_task_finally_completed Bool)
(declare-const admitted_working Bool)
(declare-const admitted_rejection_close_requested Bool)

(declare-const rejected_task_admitted Bool)
(declare-const rejected_task_finally_completed Bool)
(declare-const rejected_working Bool)
(declare-const rejected_rejection_close_requested Bool)

(assert (! (= admitted_task_admitted true)
           :named admitted_fixture))
(assert (! (= admitted_task_finally_completed false)
           :named admitted_task_still_running))
(assert (! (= admitted_working
              (and admitted_task_admitted
                   (not admitted_task_finally_completed)))
           :named admitted_working_definition))
(assert (! (= admitted_rejection_close_requested false)
           :named admitted_path_has_no_rejection_recovery))

(assert (! (= rejected_task_admitted false)
           :named rejected_fixture))
(assert (! (= rejected_task_finally_completed false)
           :named rejected_task_has_no_finally))
(assert (! (= rejected_working
              (and rejected_task_admitted
                   (not rejected_task_finally_completed)))
           :named rejected_working_definition))
(assert (! (= rejected_rejection_close_requested true)
           :named rejected_path_requests_close))

(assert (! admitted_working
           :named admitted_work_remains_reachable))
(assert (! (and (not rejected_working)
                rejected_rejection_close_requested)
           :named rejection_recovery_remains_reachable))
