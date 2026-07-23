; Non-vacuity control: an exact current identity is processed.
; Chiasmus adds check-sat/model commands.
(declare-const event_fd Int)
(declare-const event_generation Int)
(declare-const current_fd Int)
(declare-const current_generation Int)
(declare-const selector_process Bool)

(assert (and (<= 0 event_fd) (<= event_fd 3)))
(assert (and (<= 0 current_fd) (<= current_fd 3)))
(assert (and (<= 0 event_generation) (<= event_generation 3)))
(assert (and (<= 0 current_generation) (<= current_generation 3)))
(assert (= selector_process
           (and (= event_fd current_fd)
                (= event_generation current_generation))))
(assert (= event_fd current_fd))
(assert (= event_generation current_generation))
(assert selector_process)
