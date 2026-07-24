; Known-SAT split-check control for accept publication versus stop.
;
; The accept path observes :open, stop wins before publication, but the stale
; observation still publishes the Context and submits its handler.
(declare-const registration_acquired Bool)
(declare-const accepting_observed_before_stop Bool)
(declare-const stop_cas_before_publication Bool)
(declare-const context_published Bool)
(declare-const handler_submitted Bool)
(declare-const registration_rolled_back Bool)
(declare-const violation Bool)

(assert (! registration_acquired :named registered_socket_fixture))
(assert (! accepting_observed_before_stop :named stale_open_observation))
(assert (! stop_cas_before_publication :named stop_wins_after_check))

(assert (! (= context_published
              (and registration_acquired accepting_observed_before_stop))
           :named split_check_publication_definition))
(assert (! (= handler_submitted context_published)
           :named publication_submits_handler))
(assert (! (= registration_rolled_back
              (and registration_acquired (not context_published)))
           :named rollback_only_when_unpublished))
(assert (! (= violation
              (and stop_cas_before_publication
                   context_published
                   handler_submitted
                   (not registration_rolled_back)))
           :named post_stop_publication_definition))

(assert (! violation :named post_stop_publication_query))
