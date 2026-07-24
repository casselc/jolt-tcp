; Known-SAT control for the exact connector deadline violation query.
; A faulty connector gives each candidate the original relative 10 ms budget,
; so candidate 2 can wait 6 ms after candidate 1 already consumed 6 ms.

(declare-const deadline Int)
(declare-const start1 Int)
(declare-const start2 Int)
(declare-const start3 Int)
(declare-const request1 Int)
(declare-const request2 Int)
(declare-const request3 Int)
(declare-const duration1 Int)
(declare-const duration2 Int)
(declare-const duration3 Int)
(declare-const finish Int)
(declare-const violation Bool)

(assert (! (= deadline 10) :named fixed_deadline))
(assert (! (= start1 0) :named first_start))
(assert (! (= request1 deadline) :named buggy_first_relative_wait))
(assert (! (= duration1 6) :named first_candidate_duration))
(assert (! (= start2 (+ start1 duration1)) :named second_start))
(assert (! (= request2 deadline) :named buggy_second_deadline_reset))
(assert (! (= duration2 6) :named second_candidate_duration))
(assert (! (= start3 (+ start2 duration2)) :named third_start))
(assert (! (= request3 deadline) :named buggy_third_deadline_reset))
(assert (! (= duration3 0) :named third_candidate_duration))
(assert (! (= finish (+ start3 duration3)) :named finish_time))

; Same asserted violation schema as the corrected model.
(assert (! (= violation
              (or (not (= request1 (- deadline start1)))
                  (not (= request2
                          (ite (> (- deadline start2) 0)
                               (- deadline start2) 0)))
                  (not (= request3
                          (ite (> (- deadline start3) 0)
                               (- deadline start3) 0)))
                  (> finish deadline)))
           :named violation_definition))
(assert (! violation :named violation_query))
