; Claim: within three resolver candidates, every wait request is derived from
; one lexical absolute deadline, and candidate work cannot consume more than
; the remaining budget. The query asks for a reset wait or bounded overrun.

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

(assert (! (and (>= deadline 3) (<= deadline 100))
           :named bounded_deadline))
(assert (! (= start1 0) :named first_start))

; remaining-ms(deadline, now), abstracted to integral milliseconds.
(assert (! (= request1 (- deadline start1))
           :named first_wait_uses_absolute_deadline))
(assert (! (and (>= duration1 1) (< duration1 request1))
           :named first_candidate_finishes_with_budget))
(assert (! (= start2 (+ start1 duration1)) :named second_start))
(assert (! (= request2 (- deadline start2))
           :named second_wait_uses_same_absolute_deadline))
(assert (! (and (>= duration2 1) (< duration2 request2))
           :named second_candidate_finishes_with_budget))
(assert (! (= start3 (+ start2 duration2)) :named third_start))
(assert (! (= request3 (- deadline start3))
           :named third_wait_uses_same_absolute_deadline))
(assert (! (and (>= duration3 0) (<= duration3 request3))
           :named third_duration_is_bounded_by_remaining))
(assert (! (= finish (+ start3 duration3)) :named finish_time))

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
