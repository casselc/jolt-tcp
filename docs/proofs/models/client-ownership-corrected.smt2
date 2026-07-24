; Claim: within three candidates, only a successfully transferred socket and
; its connector poller remain open. Exhaustion/timeout leaves neither open.

(declare-const outcome Int)
(declare-const attempted1 Bool)
(declare-const attempted2 Bool)
(declare-const attempted3 Bool)
(declare-const socket1_open Bool)
(declare-const socket2_open Bool)
(declare-const socket3_open Bool)
(declare-const poller_open Bool)
(declare-const poller_transferred Bool)
(declare-const violation Bool)

; outcome 0 means exhaustion/timeout; 1..3 names the successful candidate.
(assert (! (and (>= outcome 0) (<= outcome 3))
           :named bounded_outcome))
(assert (! (= attempted1 true) :named first_candidate_is_attempted))
(assert (! (= attempted2 (or (= outcome 0) (= outcome 2) (= outcome 3)))
           :named second_candidate_attempt_definition))
(assert (! (= attempted3 (or (= outcome 0) (= outcome 3)))
           :named third_candidate_attempt_definition))

; retire-attempt! closes every non-selected socket. Success transfers exactly
; the selected socket; dial-candidates!'s finally closes an untransferred poller.
(assert (! (= socket1_open (= outcome 1))
           :named socket1_ownership))
(assert (! (= socket2_open (= outcome 2))
           :named socket2_ownership))
(assert (! (= socket3_open (= outcome 3))
           :named socket3_ownership))
(assert (! (= poller_open (> outcome 0))
           :named poller_ownership))
(assert (! (= poller_transferred (> outcome 0))
           :named poller_transfer))

(assert (! (= violation
              (or
                (and attempted1 (not (= outcome 1)) socket1_open)
                (and attempted2 (not (= outcome 2)) socket2_open)
                (and attempted3 (not (= outcome 3)) socket3_open)
                (and (not attempted1) socket1_open)
                (and (not attempted2) socket2_open)
                (and (not attempted3) socket3_open)
                (and (= outcome 0) poller_open)
                (and (= outcome 0) poller_transferred)
                (and (> outcome 0)
                     (or (not poller_open)
                         (not poller_transferred)
                         (and (= outcome 1) (not socket1_open))
                         (and (= outcome 2) (not socket2_open))
                         (and (= outcome 3) (not socket3_open))))))
           :named violation_definition))
(assert (! violation :named violation_query))
