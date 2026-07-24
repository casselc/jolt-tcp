; Known-SAT control for the task-tracker retention invariant.
;
; Fault injected: completed task promises remain in an append-only history.
; Freeze closes admission and every task completes, but the retained set never
; shrinks, so cleanup cannot observe the stable-empty boundary.
(declare-const active_before_freeze Int)
(declare-const completed_after_freeze Int)
(declare-const accepting_after_freeze Bool)
(declare-const late_register_attempted Bool)
(declare-const late_task_admitted Bool)
(declare-const active_at_boundary Int)
(declare-const violation Bool)

(assert (! (= active_before_freeze 2)
           :named active_tasks_fixture))
(assert (! (= completed_after_freeze active_before_freeze)
           :named all_active_tasks_complete))
(assert (! (= accepting_after_freeze false)
           :named freeze_closes_admission))
(assert (! late_register_attempted
           :named late_register_fixture))
(assert (! (= late_task_admitted
              (and accepting_after_freeze late_register_attempted))
           :named late_admission_definition))

; Faulty implementation retains every completion promise.
(assert (! (= active_at_boundary
              (+ active_before_freeze
                 (ite late_task_admitted 1 0)))
           :named append_only_history_fault))
(assert (! (= violation
              (or accepting_after_freeze
                  late_task_admitted
                  (not (= active_at_boundary 0))))
           :named violation_definition))
(assert (! violation :named violation_query))
