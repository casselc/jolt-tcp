; Bounded selector model from the reactor's pending/conns identity.
; Chiasmus adds check-sat/model/core commands.
; Domain: raw fd and generation are integers 0..3.
(declare-const event_fd Int)
(declare-const event_generation Int)
(declare-const current_fd Int)
(declare-const current_generation Int)
(declare-const selector_process Bool)
(declare-const violation Bool)

(assert (! (and (<= 0 event_fd) (<= event_fd 3))
           :named event_fd_in_domain))
(assert (! (and (<= 0 current_fd) (<= current_fd 3))
           :named current_fd_in_domain))
(assert (! (and (<= 0 event_generation) (<= event_generation 3))
           :named event_generation_in_domain))
(assert (! (and (<= 0 current_generation) (<= current_generation 3))
           :named current_generation_in_domain))
(assert (! (= selector_process
              (and (= event_fd current_fd)
                   (= event_generation current_generation)))
           :named exact_identity_selector))
(assert (! (= violation
              (and (= event_fd current_fd)
                   (not (= event_generation current_generation))
                   selector_process))
           :named violation_definition))
(assert (! violation :named queried_stale_event_processed))
