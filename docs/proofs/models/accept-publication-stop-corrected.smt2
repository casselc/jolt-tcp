; Counterexample query for the CAS gate shared by accept publication and stop.
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
(declare-const post_stop_publication Bool)
(declare-const stop_first_resource_escape Bool)
(declare-const violation Bool)

(assert (! registration_acquired :named registered_socket_fixture))
(assert (! (= phase_at_publication
              (ite (= gate_order publication_first) open stopping))
           :named gate_order_defines_phase))
(assert (! (= stop_cas_before_publication
              (= gate_order stop_first))
           :named stop_order_definition))

; with-accept-publication reserves WORKING and publishes the registry entry
; while it owns the same gate as request-stop!. Executor submission follows
; after the gate is released; it may race a later stop, but cleanup already sees
; the published Context as owned by that handler.
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
(assert (! (= post_stop_publication
              (and stop_cas_before_publication
                   (or context_published handler_submitted)))
           :named post_stop_publication_definition))
(assert (! (= stop_first_resource_escape
              (and stop_cas_before_publication
                   registration_acquired
                   (not registration_rolled_back)))
           :named stop_first_resource_escape_definition))
(assert (! (= violation
              (or post_stop_publication
                  stop_first_resource_escape
                  (and context_published (not working_reserved))))
           :named violation_definition))

(assert (! violation :named violation_query))
