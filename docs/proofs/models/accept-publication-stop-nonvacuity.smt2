; Reachability control for the stop-first branch of gated accept publication.
(declare-datatypes () ((GateOrder publication_first stop_first)))
(declare-datatypes () ((AdmissionPhase open stopping)))
(declare-const gate_order GateOrder)
(declare-const phase_at_publication AdmissionPhase)
(declare-const registration_acquired Bool)
(declare-const stop_cas_before_publication Bool)
(declare-const context_published Bool)
(declare-const working_reserved Bool)
(declare-const handler_submitted Bool)
(declare-const registration_rolled_back Bool)

(assert (! registration_acquired :named registered_socket_fixture))
(assert (! (= gate_order stop_first) :named stop_first_fixture))
(assert (! (= phase_at_publication
              (ite (= gate_order publication_first) open stopping))
           :named gate_order_defines_phase))
(assert (! (= stop_cas_before_publication
              (= gate_order stop_first))
           :named stop_order_definition))
(assert (! (= context_published
              (and registration_acquired
                   (= phase_at_publication open)))
           :named gated_publication_definition))
(assert (! (= working_reserved context_published)
           :named publication_reserves_working))
(assert (! (= handler_submitted context_published)
           :named published_context_is_submitted_after_gate))
(assert (! (= registration_rolled_back
              (and registration_acquired (not context_published)))
           :named unpublished_registration_rolls_back))

(assert (! (and stop_cas_before_publication
                (= phase_at_publication stopping)
                (not context_published)
                (not working_reserved)
                (not handler_submitted)
                registration_rolled_back)
           :named stop_first_rollback_trace_reachable))
