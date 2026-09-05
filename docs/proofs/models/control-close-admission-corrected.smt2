; Counterexample query for the corrected implementation.
; Concatenate after control-close-admission.smt2.
(assert (! gate_enabled :named shared_control_admission_gate))
(assert (! closed_drain_enabled :named closed_path_drains_controls))
(assert (! fifo_queue_enabled :named fifo_queue_head_poll))
(assert (! violation :named violation_query))
