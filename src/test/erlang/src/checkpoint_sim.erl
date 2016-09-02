
-module(checkpoint_sim).

-compile(export_all).

%% N = # of times to repeat the Keys + Vals put sequence,
%%     plus # of times to repeat deleting each key,
%%     plus # of times to clear the table with 'del_all'

make_random_cmd_list_with_cps(N, Keys, Vals, CPs) ->
    expand_cmds(random_cmd_list(N, Keys, Vals, CPs)).

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

random_cmd_list(N, Keys, Vals, CPs) ->
    shuffle(cmds(N, Keys, Vals) ++ checkpoint_cmds(CPs)).

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

l_split(Num, L) ->
    try
        lists:split(Num, L)
    catch
        error:badarg ->
            {L, []}
    end.

key(N) ->
    "key" ++ integer_to_list(N).

val(N) ->
    "val" ++ integer_to_list(N).

shuffle(L) ->
    [X || {_, X} <- lists:sort([{random:uniform(999999), Y} || Y <- L])].

