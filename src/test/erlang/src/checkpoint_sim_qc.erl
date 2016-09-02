%% To compile and run:
%%
%% cd $CORFU_TOP/src/test/erlang
%% env PROPER_DIR=/your/path/to/proper-source-repo ./Build.sh proper
%% env PROPER_DIR=/your/path/to/proper-source-repo ./Build.sh proper-shell
%% proper:quickcheck(proper:numtests(250, checkpoint_sim_qc:prop_all_cmd_prefixes())).

-module(checkpoint_sim_qc).

-ifdef(PROPER).
-include_lib("proper/include/proper.hrl").
-endif.

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

prop_all_cmd_prefixes() ->
    %% N = # of times to repeat the Keys + Vals put sequence,
    %%     plus # of times to repeat deleting each key,
    %%     plus # of times to clear the table with 'del_all'
    %% Keys = Number of unique keys
    %% Vals = Number of unique values per key
    %% CPs = Number of checkpoints to insert into the simulated log
    %% Seed = Random number generator seed value (give QC over randomness)
    ?FORALL({N, Keys, Vals, CPs, Seed},
            {choose(1,4), choose(1,10), choose(0,20), choose(1,5),
             noshrink({choose(1, 5000),choose(1, 5000),choose(1, 5000)})},
            begin
                random:seed(Seed),              % Control non-QC random # gen.

                %% Create the master list of m + cp commands.
                Cmds = checkpoint_sim:make_random_cmd_list_with_cps(N, Keys,
                                                                    Vals, CPs),
                %% For each prefix of the master list, we execute the
                %% loop below and accumulate the results.  For each
                %% prefix, the result will be 'true' or Else, where
                %% Else is some debugging info to help piece together
                %% what went wrong.
                AllPrefixResults =
                    [begin
                         %% Take our command prefix
                         CmdsP = lists:sublist(Cmds, PrefixLength),
                         %% Simulate backpointer chasing over the cmd prefix
                         {CP_Ms, Ms} = chase_backpointers_to_latest_cp(CmdsP),
                         %% Simulate SMR-style log replay with checkpoints
                         D_with_cps = checkpoint_sim:apply_log(CP_Ms ++ Ms),
                         %% Simulate SMR-style log replay without checkpoints
                         D_no_cps = checkpoint_sim:apply_log(filter_cps(CmdsP)),

                         %% Extract key-value pairs from each map.
                         %% We know that orddict:to_list(D) results are sorted
                         NoCPs = orddict:to_list(D_no_cps),
                         WithCPs = orddict:to_list(D_with_cps),

                         %% Sanity check: the sequence of m cmds from the
                         %% command prefix is exactly equal to the sequence
                         %% that the simulated backpointer chasing finds.
                         Ms_only = filter_cps(CmdsP),
                         Ms_in_exact_order = (Ms_only == Ms),

                         %% Sanity check: backpointer chasing must find
                         %% at least one m cmd, orelse we shrunk the cmd
                         %% prefix so small that it contains only
                         %% checkpoint cmds.
                         Ms_extra_sanity =
                             Ms /= []
                             orelse
                             filter_cps(CmdsP) == [],

                         %% Sanity check: 1st clause: Our key-value pair
                         %% lists are exactly the same, without and with
                         %% cp cmds.
                         if NoCPs == WithCPs
                            andalso
                            Ms_in_exact_order
                            andalso
                            Ms_extra_sanity ->
                                 true;
                            true ->
                                 %% Bummer.  Pass along debugging info.
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
                D_no_cps_full_list = checkpoint_sim:apply_log(filter_cps(Cmds)),

                %% QuickCheck test result formatting stuff: when the test
                %% fails (?WHENFAIL()) and when it succeeds (measure()
                %% and collect()).  Impatient readers should skip ahead
                %% to "Here is our final test criteria".
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
                        %% Here is our final test criteria.  Any
                        %% exception in the sanity checking above means
                        %% that we never got here ... but QuickCheck will
                        %% notice those exceptions also.
                        %%
                        %% The only value in all of AllPrefixResults must
                        %% be 'true'
                        lists:usort(AllPrefixResults) == [true]
                  )))))))
            end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

filter_cps(Cmds) ->
    [X || X <- Cmds, element(1, X) /= cp].

chase_backpointers_to_latest_cp(Cmds) ->
    %% Use lists:reverse() here to simulate following backpointers from
    %% the __end__ of the log.
    chase_backpointers_to_latest_cp(lists:reverse(Cmds), no_name, [], []).

chase_backpointers_to_latest_cp([], _ObservedCP, Dumps, Ms) ->
    %% We have reached the end of the log, real (it is the end of our cmd
    %% sequence simulated log) or virtual (another clause is faking
    %% end-of-log because it found the beginning of the last successful
    %% checkpoint in the sequence/log).
    %%
    %% Do not use lists:reverse() here because our input, the log,
    %% was reversed to simulate backpointer chasing.
    %%
    %% We do, however, need to remove the dorky 'start' atom from the
    %% start of the checkpoint mutation history.
    {lists:append(Dumps -- [start]), Ms};
chase_backpointers_to_latest_cp([{m,_}=M|Rest], ObservedCP, Dumps, Ms) ->
    %% Save all m records.
    chase_backpointers_to_latest_cp(Rest, ObservedCP, Dumps, [M|Ms]);
chase_backpointers_to_latest_cp([{cp,CP_Name,ok}|Rest], no_name, Dumps, Ms) ->
    %% This cp finished successfully, and it is the first one that we
    %% have seen (2nd arg is `no_name`).  Hooray, let's start
    %% accumulating checkpoint dumps....
    chase_backpointers_to_latest_cp(Rest, CP_Name, Dumps, Ms);
chase_backpointers_to_latest_cp([{cp,CP_Name,Dump}|Rest], CP_Name, Dumps, Ms) ->
    %% Save this dump record for our chckpoint.
    chase_backpointers_to_latest_cp(Rest, CP_Name, [Dump|Dumps], Ms);
chase_backpointers_to_latest_cp([{cp,_,_}|Rest], CP_Name, Dumps, Ms) ->
    %% Some other checkpoint: ignore it.
    chase_backpointers_to_latest_cp(Rest, CP_Name, Dumps, Ms).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

has_mods_during_cp_p(Cmds) ->
    Good_cp_names = [Name || {cp, Name, ok} <- Cmds],
    lists:member(true,
                 [has_mods_during_cp(Name, Cmds) || Name <- Good_cp_names]).

has_mods_during_cp(Name, Cmds) ->
    %% Find suffix that starts with {cp,_,start}.
    Suff = lists:dropwhile(fun({cp, N, start}) when N == Name -> false;
                              (_)                             -> true
                           end, Cmds),
    %% Find middle that ends with {cp,_,ok}.
    Middle = lists:takewhile(fun({cp, N, ok}) when N == Name -> false;
                                (_)                          -> true
                             end, Suff),
    %% Extract m's from the middle.
    [X || X={m,_} <- Middle] /= [].

has_overlapping_cps_p(Cmds) ->
    %% Create a mapping of the number of concurrent checkpoints.
    %% If the number goes above one, then the answer is true.
    Is = lists:map(fun({cp, _, start})                     ->  1;
                      ({cp, _, X}) when X == ok; X == fail -> -1;
                      (_)                                  ->  0
                   end, Cmds),
    X = lists:foldl(fun(_, true) -> true;
                       (1, 1)    -> true;
                       (X, Y) -> X + Y
                    end, 0, Is),
    X == true.

