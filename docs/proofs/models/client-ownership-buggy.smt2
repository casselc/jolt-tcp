; Known-SAT control for the connector ownership violation query.
; All candidates fail, but a faulty cleanup leaves candidate 1 and the connector
; poller open.

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

(assert (! (= outcome 0) :named exhausted))
(assert (! (= attempted1 true) :named candidate1_attempted))
(assert (! (= attempted2 true) :named candidate2_attempted))
(assert (! (= attempted3 true) :named candidate3_attempted))
(assert (! (= socket1_open true) :named buggy_failed_socket_leak))
(assert (! (= socket2_open false) :named socket2_closed))
(assert (! (= socket3_open false) :named socket3_closed))
(assert (! (= poller_open true) :named buggy_failed_poller_leak))
(assert (! (= poller_transferred false) :named no_failure_transfer))

; outcome 0 means failure; 1..3 name the successful candidate.
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
