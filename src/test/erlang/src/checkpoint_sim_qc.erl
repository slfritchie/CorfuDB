%% To compile and run:
%%
%% cd $CORFU_TOP/src/test/erlang
%% env PROPER_DIR=/your/path/to/proper-source-repo ./Build.sh proper
%% env PROPER_DIR=/your/path/to/proper-source-repo ./Build.sh proper-shell
%% proper:quickcheck(proper:numtests(250, checkpoint_sim:prop_all_cmd_prefixes())).

-module(checkpoint_sim_qc).

-ifdef(PROPER).
-include_lib("proper/include/proper.hrl").
-endif.

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

prop_all_cmd_prefixes() ->
    ?FORALL({N, Keys, Vals, CPs, Seed},
            {choose(1,4), choose(1,10), choose(0,20), choose(1,5),
             noshrink({choose(1, 5000),choose(1, 5000),choose(1, 5000)})},
            begin
                random:seed(Seed),
                Cmds = checkpoint_sim:make_random_cmd_list_with_cps(N, Keys,
                                                                    Vals, CPs),
                D_no_cps_full_list = checkpoint_sim:apply_log(filter_cps(Cmds)),
                
                AllPrefixResults =
                    [begin
                         CmdsP = lists:sublist(Cmds, PrefixLength),
                         Ms_only = filter_cps(CmdsP),
                         {CP_Ms, Ms} = chase_backpointers_to_latest_cp(CmdsP),
                         D_with_cps = checkpoint_sim:apply_log(CP_Ms ++ Ms),
                         D_no_cps = checkpoint_sim:apply_log(filter_cps(CmdsP)),

                         %% We know the results of orddict:to_list(D) are sorted
                         NoCPs = orddict:to_list(D_no_cps),
                         WithCPs = orddict:to_list(D_with_cps),
                         Ms_in_exact_order = (Ms_only == Ms),
                         Ms_extra_sanity =
                             Ms /= []
                             orelse
                             %% If our commands prefix is all checkpoint
                             %% entries, then it's ok for our Ms list to be
                             %% empty.
                             filter_cps(CmdsP) == [],

                         if NoCPs == WithCPs
                            andalso
                            Ms_in_exact_order
                            andalso
                            Ms_extra_sanity ->
                                 true;
                            true ->
                                 [{no_cps, NoCPs},
                                  {with_cps,WithCPs},
                                  {cp_ms, CP_Ms},
                                  {ms, Ms}]
                         end
                     end || PrefixLength <- lists:seq(1, length(Cmds))],

                %% For use with QC's measure() & collect() calcs.
                OK_CPs = [X || X={cp,_,ok} <- Cmds],
                Fail_CPs = [X || X={cp,_,fail} <- Cmds],
                HasModsDuringCP_p = has_mods_during_cp_p(Cmds),
                HasOverlappingCPs_p = has_overlapping_cps_p(Cmds),

                ?WHENFAIL(
                io:format(user, "N = ~p, Keys = ~p, Vals = ~p, CPs = ~p\n"
                                "Cmds = ~p\n"
                                "Res = ~p\n", [N, Keys, Vals, CPs, Cmds,
                                              AllPrefixResults]),
                measure(cmd_length, length(Cmds),
                measure(successful_cps, length(OK_CPs),
                measure(fail_cps, length(Fail_CPs),
                measure(num_unique_keys, length(D_no_cps_full_list),
                collect(with_title("Has mods during CP"), HasModsDuringCP_p,
                collect(with_title("Has overlapping CPs"), HasOverlappingCPs_p,
                conjunction(
                  [
                   {all_true, lists:usort(AllPrefixResults) == [true]}
                  ]))))))))
            end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

filter_cps(Cmds) ->
    [X || X <- Cmds, element(1, X) /= cp].

chase_backpointers_to_latest_cp(Cmds) ->
    %% Use lists:reverse() here to simulate following backpointers from
    %% the end of the log.
    chase_backpointers_to_latest_cp(lists:reverse(Cmds), no_name, [], []).

chase_backpointers_to_latest_cp([], _ObservedCP, Dumps, Ms) ->
    %% Do not use lists:reverse() here because our input, the log,
    %% was reversed to simulate backpointer chasing.
    %%
    %% We do, however, need to remove the dorky 'start' atom from the
    %% start of the checkpoint mutation history.
    {lists:append(Dumps -- [start]), Ms};
chase_backpointers_to_latest_cp([{m,_}=M|Rest], ObservedCP, Dumps, Ms) ->
    chase_backpointers_to_latest_cp(Rest, ObservedCP, Dumps, [M|Ms]);
chase_backpointers_to_latest_cp([{cp,CP_Name,ok}|Rest], no_name, Dumps, Ms) ->
    chase_backpointers_to_latest_cp(Rest, CP_Name, Dumps, Ms);
chase_backpointers_to_latest_cp([{cp,CP_Name,Dump}|Rest], CP_Name, Dumps, Ms) ->
    chase_backpointers_to_latest_cp(Rest, CP_Name, [Dump|Dumps], Ms);
chase_backpointers_to_latest_cp([{cp,_,_}|Rest], CP_Name, Dumps, Ms) ->
    chase_backpointers_to_latest_cp(Rest, CP_Name, Dumps, Ms).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

has_mods_during_cp_p(Cmds) ->
    Good_cp_names = [Name || {cp, Name, ok} <- Cmds],
    lists:member(true, [has_mods_during_cp(Name, Cmds) || Name <- Good_cp_names]).

has_mods_during_cp(Name, Cmds) ->
    Suff = lists:dropwhile(fun({cp, N, start}) when N == Name -> false;
                              (_)                             -> true
                           end, Cmds),
    Middle = lists:takewhile(fun({cp, N, ok}) when N == Name -> false;
                                (_)                          -> true
                             end, Suff),
    [X || X={m,_} <- Middle] /= [].

has_overlapping_cps_p(Cmds) ->
    Is = lists:map(fun({cp, _, start})                     ->  1;
                      ({cp, _, X}) when X == ok; X == fail -> -1;
                      (_)                                  ->  0
                   end, Cmds),
    X = lists:foldl(fun(_, true) -> true;
                       (1, 1)    -> true;
                       (X, Y) -> X + Y
                    end, 0, Is),
    X == true.

