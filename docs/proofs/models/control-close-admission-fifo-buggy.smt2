; Known-SAT FIFO fault injection. Concatenate after control-close-admission.smt2.
; Admission and consumption are corrected, but q-poll! is replaced by a
; hypothetical tail-first consumer so two callbacks submit out of queue order.
(assert (! gate_enabled :named shared_control_admission_gate))
(assert (! closed_drain_enabled :named closed_path_drains_controls))
(assert (! (not fifo_queue_enabled) :named faulty_tail_first_poll))
(assert (! (not close_occurs) :named open_socket_fixture))
(assert (! (= (select attempt_step 0) 0) :named first_attempt_fixture))
(assert (! (= (select attempt_step 1) 1) :named second_attempt_fixture))
(assert (! (select queue_capacity 0) :named first_slot_available))
(assert (! (select queue_capacity 1) :named second_slot_available))
(assert (! (= (select enqueue_step 0) 2) :named first_enqueue_fixture))
(assert (! (= (select enqueue_step 1) 3) :named second_enqueue_fixture))
(assert (! (= (select release_step 0) 4) :named first_release_fixture))
(assert (! (= (select release_step 1) 5) :named second_release_fixture))
(assert (! (= (select poll_step 1) 6) :named faulty_second_polled_first))
(assert (! (= (select poll_step 0) 7) :named faulty_first_polled_second))
(assert (! violation :named violation_query))
