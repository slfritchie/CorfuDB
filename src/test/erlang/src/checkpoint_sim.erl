-module(checkpoint_sim).

-include_lib("proper/include/proper.hrl").

-compile(export_all).

prop_all_cmd_prefixes() ->
    ?FORALL({N, Keys, Vals, CPs, Seed},
            {choose(1,4), choose(1,10), choose(0,20), choose(1,5),
             noshrink({choose(1, 5000),choose(1, 5000),choose(1, 5000)})},
            begin
                random:seed(Seed),
                Cmds = make_random_cmd_list_with_cps(N, Keys, Vals, CPs),
                D_no_cps_full_list = apply_log(filter_cps(Cmds)),
                
                AllPrefixResults =
                    [begin
                         CmdsP = lists:sublist(Cmds, PrefixLength),
                         Ms_only = filter_cps(CmdsP),
                         {CP_Ms, Ms} = chase_backpointers_to_latest_cp(CmdsP),
                         D_with_cps = apply_log(CP_Ms ++ Ms),
                         D_no_cps   = apply_log(filter_cps(CmdsP)),

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

%% N = # of times to repeat the Keys + Vals put sequence,
%%     plus # of times to repeat deleting each key,
%%     plus # of times to clear the table with 'del_all'

make_random_cmd_list_with_cps(N, Keys, Vals, CPs) ->
    checkpoint_sim:expand_cmds(
      checkpoint_sim:random_cmd_list(N, Keys, Vals, CPs)).

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

apply_log(Cmds) ->
    lists:foldl(fun apply_mutation/2, orddict:new(), Cmds).

apply_mutation({m, M}, D) ->
    apply_mutation(M, D);
apply_mutation(del_all, _D) ->
    orddict:new();
apply_mutation({del, K}, D) ->
    orddict:erase(K, D);
apply_mutation({put, K, V}, D) ->
    orddict:store(K, V, D).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

random_cmd_list() ->
    shuffle(cmds()).

random_cmd_list(N, Keys, Vals, CPs) ->
    shuffle(cmds(N, Keys, Vals) ++ checkpoint_cmds(CPs)).

cmds() ->
    cmds(3, 8, 4).

cmds(N, Keys, Vals) ->
    [{m, del_all} || _ <- lists:seq(1, N)] ++
    lists:append(
      lists:duplicate(N,
                      [{m, {put, key(X), val(Y)}} || X <- lists:seq(1, Keys),
                                                     Y <- lists:seq(1, Vals)]))
    ++
    lists:append(
      lists:duplicate(N,
                      [{m, {del, key(X)}} || X <- lists:seq(1, Keys)])).

checkpoint_cmds(N) ->
    %% {start_cp, # cmds to pass now, # cmds to pass intermittently,
    %%            dump batch size, final success/fail status}
    [{start_cp,
      "cp-name-" ++ integer_to_list(CP_Num),
      random:uniform(4) - 1,
      random:uniform(4) - 1,
      random:uniform(3),
      case random:uniform(100) rem 3 of
          0 -> fail;
          _ -> ok
      end} || CP_Num <- lists:seq(1, N)].

key(N) ->
    "key" ++ integer_to_list(N).

val(N) ->
    "val" ++ integer_to_list(N).

shuffle(L) ->
    [X || {_, X} <- lists:sort([{random:uniform(999999), Y} || Y <- L])].

expand_cmds(Cmds) ->
    expand_cmds(Cmds, orddict:new()).

expand_cmds([{m, Mutation}=Cmd|Rest], D) ->
    [Cmd|expand_cmds(Rest, apply_mutation(Mutation, D))];
expand_cmds([{start_cp, CP_Name, PassNow, PassInt, Batch, Status}|Rest], D) ->
    Rest2 = insert_cps(CP_Name, PassNow, PassInt, Batch, Status, Rest, D),
    expand_cmds(Rest2, D);
expand_cmds([{cp, _Name, _DumpList}=Cmd|Rest], D) ->
    [Cmd|expand_cmds(Rest, D)];
expand_cmds([], _D) ->
    [].

insert_cps(CP_Name, PassNow, PassInt, BatchNum, Status, Rest, D)
  when PassInt >= 0, BatchNum > 0 ->
    {Rest1, Rest2} = l_split(PassNow, Rest),
    %% Snapshot_KVs = shuffle(orddict:to_list(D)),
    Snapshot_KVs = orddict:to_list(D),
    [{cp, CP_Name, start}] ++ Rest1 ++
         insert_cps2(CP_Name, PassInt, PassInt, BatchNum,
                     Snapshot_KVs, Status, Rest2).

insert_cps2(CP_Name, 0, PassInt, BatchNum, [], Status, Rest) ->
    [{cp, CP_Name, Status}|
     insert_cps2(CP_Name, -1, PassInt, BatchNum, [], Status, Rest)];
insert_cps2(CP_Name, 0, PassInt, BatchNum, Snapshot_KVs, Status, Rest) ->
    {S_KV1, S_KV2} = l_split(BatchNum, Snapshot_KVs),
    Snap = {cp, CP_Name, [{put, K, V} || {K, V} <- S_KV1]},
    [Snap|insert_cps2(CP_Name, PassInt, PassInt, BatchNum, S_KV2, Status, Rest)];
insert_cps2(_CP_Name, -1, _PassInt, _BatchNum, _Snapshot_KVs, _Status, []) ->
    [];
insert_cps2(CP_Name, Pass, PassInt, BatchNum, Snapshot_KVs, Status, []) ->
    NewPass = if Pass >= 0 -> 0;
                 true      -> -1
              end,
    insert_cps2(CP_Name, NewPass, PassInt, BatchNum, Snapshot_KVs, Status, []);
insert_cps2(CP_Name, Pass, PassInt, BatchNum, Snapshot_KVs, Status, [H|Rest]) ->
    [H|insert_cps2(CP_Name, Pass - 1, PassInt, BatchNum,
                   Snapshot_KVs, Status, Rest)].

l_split(Num, L) ->
    try
        lists:split(Num, L)
    catch
        error:badarg ->
            {L, []}
    end.

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

