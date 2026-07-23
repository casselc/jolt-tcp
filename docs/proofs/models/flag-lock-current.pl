% Current source-derived call graph: set_flag performs an atomic swap.
% Queries:
%   reaches(write, set_flag).                                  => true
%   recursive_acquisition(write, set_flag, socket_lock).       => no answers
edge(write, queue_write).
edge(queue_write, set_flag).
edge(set_flag, atomic_flags_swap).

acquires(write, socket_lock).

:- table reaches/2.
reaches(From, To) :- edge(From, To).
reaches(From, To) :- edge(From, Next), reaches(Next, To).

recursive_acquisition(Outer, Inner, Lock) :-
    acquires(Outer, Lock),
    reaches(Outer, Inner),
    acquires(Inner, Lock).
