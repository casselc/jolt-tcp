; Bounded stable-empty task-tracker counterexample query.
;
; Domain: zero to four tasks are active when cleanup freezes admission. Every
; one completes within the bound. A late registration is attempted after the
; freeze. The source transitions remove each completed promise from :active and
; reject registration once :accepting? is false.
(declare-const active_before_freeze Int)
(declare-const completed_after_freeze Int)
(declare-const accepting_after_freeze Bool)
(declare-const late_register_attempted Bool)
(declare-const late_task_admitted Bool)
(declare-const active_at_boundary Int)
(declare-const violation Bool)

(assert (! (and (<= 0 active_before_freeze)
                (<= active_before_freeze 4))
           :named active_task_bound))
(assert (! (= completed_after_freeze active_before_freeze)
           :named all_active_tasks_complete_within_bound))
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
(assert (! (= violation
              (or accepting_after_freeze
                  late_task_admitted
                  (not (= active_at_boundary 0))))
           :named violation_definition))
(assert (! violation :named violation_query))
