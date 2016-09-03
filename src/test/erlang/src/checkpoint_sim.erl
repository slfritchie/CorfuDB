
-module(checkpoint_sim).

-compile(export_all).

%% N = # of times to repeat the Keys + Vals put sequence,
%%     plus # of times to repeat deleting each key,
%%     plus # of times to clear the table with 'del_all'
%% Keys = Number of unique keys
%% Vals = Number of unique values per key
%% CPs = Number of checkpoints to insert into the simulated log

make_random_cmd_list_with_cps(N, Keys, Vals, CPs) ->
    expand_cmds(random_cmd_list(N, Keys, Vals, CPs)).

%% Apply simulated log entries to an SMR-style map.  Assume the map is
%% empty at the start of the cmd sequence.

apply_log(Cmds) ->
    lists:foldl(fun apply_mutation/2, orddict:new(), Cmds).

apply_mutation({m, M}, D) ->
    apply_mutation(M, D);                   % Strip {m, M} wrapper & retry
apply_mutation(del_all, _D) ->
    orddict:new();
apply_mutation({del, K}, D) ->
    orddict:erase(K, D);
apply_mutation({put, K, V}, D) ->
    orddict:store(K, V, D).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Helper functions
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

expand_cmds(Cmds) ->
    expand_cmds(Cmds, orddict:new()).

expand_cmds([{start_cp, CP_Name, PassNow, PassInt, Batch, Status}|Rest], D) ->
    %% Remove this tuple and insert a bunch of checkpoint items.
    Rest2 = insert_cps(CP_Name, PassNow, PassInt, Batch, Status, Rest, D),
    expand_cmds(Rest2, D);
expand_cmds([{cp, _Name, _DumpList}=Cmd|Rest], D) ->
    %% Keep this record.
    [Cmd|expand_cmds(Rest, D)];
expand_cmds([{m, Mutation}=Cmd|Rest], D) ->
    %% Keep this record.  Also apply the mutation to our local dict.
    [Cmd|expand_cmds(Rest, apply_mutation(Mutation, D))];
expand_cmds([], _D) ->
    [].

insert_cps(CP_Name, _PassNow, _PassInt, BatchNum, Status, Rest, D)
  when BatchNum > 0 ->
    CPs = make_cp_dumps_and_end(CP_Name, BatchNum, Status, D),
    %% We want the {cp,_,start} record to appear immediately, so
    %% don't let it slip into random_merge()'s args.
    [{cp, CP_Name, start}] ++ random_merge(CPs, Rest).

make_cp_dumps_and_end(CP_Name, BatchNum, Status, D) ->
    %% Shuffle up the order of entries in our dict, then make a bunch
    %% of dump records, then put a status record at the end.
    make_cp_dumps(shuffle(orddict:to_list(D)), CP_Name, BatchNum)
        ++ [{cp, CP_Name, Status}].

make_cp_dumps([], _, _) ->
    [];
make_cp_dumps(KVs, CP_Name, BatchNum) ->
    {Now_KVs, Later_KVs} = l_split(BatchNum, KVs),
    [{cp, CP_Name, [{put, K, V} || {K, V} <- Now_KVs]}] ++
        make_cp_dumps(Later_KVs, CP_Name, BatchNum).

random_merge([], []) ->
    [];
random_merge(Rest1, []) ->
    Rest1;
random_merge([], Rest2) ->
    Rest2;
random_merge([H1|Rest1]=L1, [H2|Rest2]=L2) ->
    case random:uniform(100) of
        N when N < 50 ->
            [H1|random_merge(Rest1, L2)];
        _ ->
            [H2|random_merge(L1, Rest2)]
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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

