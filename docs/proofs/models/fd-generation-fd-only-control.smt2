; Known-SAT control: raw fd alone is treated as connection identity.
; Chiasmus adds check-sat/model commands.
(declare-const event_fd Int)
(declare-const event_generation Int)
(declare-const current_fd Int)
(declare-const current_generation Int)
(declare-const selector_process Bool)
(declare-const violation Bool)

(assert (and (<= 0 event_fd) (<= event_fd 3)))
(assert (and (<= 0 current_fd) (<= current_fd 3)))
(assert (and (<= 0 event_generation) (<= event_generation 3)))
(assert (and (<= 0 current_generation) (<= current_generation 3)))
(assert (= selector_process (= event_fd current_fd)))
(assert (= violation
           (and (= event_fd current_fd)
                (not (= event_generation current_generation))
                selector_process)))
(assert violation)
