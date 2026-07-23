; Non-vacuity boundary: both an old and a late byte are delivered before EOF.
; Chiasmus adds check-sat/model commands.
(declare-const old_bytes Int)
(declare-const late_bytes Int)
(declare-const total_pre_eof_bytes Int)
(declare-const close_after_terminal_push Bool)
(declare-const channel_closed Bool)
(declare-const delivered_bytes Int)

(assert (! (= old_bytes 1) :named one_old_byte))
(assert (! (= late_bytes 1) :named one_late_byte))
(assert (! (= total_pre_eof_bytes (+ old_bytes late_bytes))
           :named total_pre_eof_definition))
(assert (! close_after_terminal_push
           :named peer_eof_notified_orders_close_after_push))
(assert (! channel_closed :named end_of_stream_exposed))
(assert (! (= delivered_bytes
              (+ old_bytes (ite close_after_terminal_push late_bytes 0)))
           :named delivered_bytes_definition))
(assert (! (= delivered_bytes total_pre_eof_bytes)
           :named all_pre_eof_bytes_delivered))
