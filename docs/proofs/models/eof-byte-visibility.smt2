; Bounded byte-visibility model for the stream EOF notification boundary.
; Chiasmus adds check-sat/model/core commands.
(declare-const old_bytes Int)
(declare-const late_bytes Int)
(declare-const total_pre_eof_bytes Int)
(declare-const close_after_terminal_push Bool)
(declare-const channel_closed Bool)
(declare-const delivered_bytes Int)
(declare-const violation Bool)

(assert (! (and (<= 0 old_bytes) (<= old_bytes 8))
           :named old_bytes_in_domain))
(assert (! (and (<= 1 late_bytes) (<= late_bytes 8))
           :named late_bytes_in_domain))
(assert (! (= total_pre_eof_bytes (+ old_bytes late_bytes))
           :named total_pre_eof_definition))
(assert (! close_after_terminal_push
           :named peer_eof_notified_orders_close_after_push))
(assert (! channel_closed :named end_of_stream_exposed))
(assert (! (= delivered_bytes
              (+ old_bytes (ite close_after_terminal_push late_bytes 0)))
           :named delivered_bytes_definition))
(assert (! (= violation
              (and channel_closed (< delivered_bytes total_pre_eof_bytes)))
           :named violation_definition))
(assert (! violation :named queried_hidden_pre_eof_bytes))
