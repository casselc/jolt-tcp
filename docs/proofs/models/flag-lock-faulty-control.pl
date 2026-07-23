% Known-faulty pre-fix call graph: set_flag reacquires the socket lock.
% Query:
%   recursive_acquisition(write, set_flag, socket_lock).       => true
edge(write, queue_write).
edge(queue_write, set_flag).
edge(set_flag, with_socket_lock).

acquires(write, socket_lock).
acquires(set_flag, socket_lock).

:- table reaches/2.
reaches(From, To) :- edge(From, To).
reaches(From, To) :- edge(From, Next), reaches(Next, To).

recursive_acquisition(Outer, Inner, Lock) :-
    acquires(Outer, Lock),
    reaches(Outer, Inner),
    acquires(Inner, Lock).
