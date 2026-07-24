; Non-vacuity control for the corrected task-tracker transitions.
;
; Two tasks are observable as active before freeze. Both complete and are
; removed, admission stays closed, a late registration is rejected, and the
; cleanup boundary is empty.
(declare-const active_before_freeze Int)
(declare-const completed_after_freeze Int)
(declare-const accepting_after_freeze Bool)
(declare-const late_register_attempted Bool)
(declare-const late_task_admitted Bool)
(declare-const active_at_boundary Int)

(assert (! (= active_before_freeze 2)
           :named nonempty_active_fixture))
(assert (! (= completed_after_freeze active_before_freeze)
           :named all_active_tasks_complete))
(assert (! (= accepting_after_freeze false)
           :named freeze_closes_admission))
(assert (! late_register_attempted
           :named late_register_fixture))
(assert (! (= late_task_admitted
              (and accepting_after_freeze late_register_attempted))
           :named register_requires_open_tracker))
(assert (! (= active_at_boundary
              (+ (- active_before_freeze completed_after_freeze)
                 (ite late_task_admitted 1 0)))
           :named completed_tasks_are_removed))

(assert (! (> active_before_freeze 0)
           :named active_work_is_reachable))
(assert (! (not late_task_admitted)
           :named late_registration_is_rejected))
(assert (! (= active_at_boundary 0)
           :named stable_empty_boundary_is_reachable))
