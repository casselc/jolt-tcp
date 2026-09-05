; Close-boundary witness for the corrected implementation.
; Concatenate after control-close-admission.smt2.
; Two producers acquire before close, enqueue in reverse acquisition order after
; the close CAS, and are drained FIFO after the active=0 barrier. A third
; producer attempts after the barrier and is synchronously rejected.
(assert (! gate_enabled :named shared_control_admission_gate))
(assert (! closed_drain_enabled :named closed_path_drains_controls))
(assert (! fifo_queue_enabled :named fifo_queue_head_poll))
(assert (! close_occurs :named close_fixture))
(assert (! (= close_cas_step 2) :named close_cas_fixture))
(assert (! (= close_barrier_step 8) :named close_barrier_fixture))
(assert (! (= closed_drain_step 9) :named closed_drain_fixture))
(assert (! (= (select attempt_step 0) 0) :named first_acquire_fixture))
(assert (! (= (select attempt_step 1) 1) :named second_acquire_fixture))
(assert (! (= (select attempt_step 2) 10) :named rejected_attempt_fixture))
(assert (! (select queue_capacity 0) :named first_capacity_fixture))
(assert (! (select queue_capacity 1) :named second_capacity_fixture))
(assert (! (select queue_capacity 2) :named rejected_capacity_irrelevant))
(assert (! (= (select enqueue_step 1) 3) :named second_acquirer_enqueues_first))
(assert (! (= (select enqueue_step 0) 4) :named first_acquirer_enqueues_second))
(assert (! (= (select release_step 1) 5) :named second_acquirer_releases_first))
(assert (! (= (select release_step 0) 6) :named first_acquirer_releases_second))
(assert (! (not (select ordinary_choice 0)) :named first_waits_for_closed_drain))
(assert (! (not (select ordinary_choice 1)) :named second_waits_for_closed_drain))
(assert (! (= (select poll_step 1) 10) :named second_acquirer_polled_first))
(assert (! (= (select poll_step 0) 11) :named first_acquirer_polled_second))
(assert (! (not violation) :named nonviolating_boundary_trace))
