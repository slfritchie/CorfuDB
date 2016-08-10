-module(layout_eqc).

%% To compile and run with Quviq's QuickCheck:
%%
%% $ erl -sname foo -pz ~/lib/eqc/ebin
%%
%% > c(layout_eqc, [{d, 'EQC'}]).
%% > eqc:quickcheck(layout_eqc:prop()).
%%
%% To compile and run with Proper:
%%
%% $ erl -sname foo -pz /Users/fritchie/src/erlang/proper/ebin
%%
%% > c(layout_eqc, [{d, 'PROPER'}]).
%% > proper:quickcheck(layout_eqc:prop()).

%% To run the corfu_server:
%% ./bin/corfu_server -Q -l /tmp/corfu-test-dir -s 8000 --cm-poll-interval=9999
%%
%% The --cm-poll-interval flag is optional: it can avoid spammy noise
%% when also using "-d TRACE" that is caused by config manager polling.

-ifdef(PROPER).
-include_lib("proper/include/proper.hrl").
-endif.

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-endif.

-compile(export_all).

-record(state, {
          reset_p = false :: boolean(),
          endpoint :: string(),
          reg_names :: list(),
          last_rank=0 :: non_neg_integer(),
          proposed_ok_rank=0 :: non_neg_integer(),
          proposed_layout="" :: string(),
          committed_rank=0 :: non_neg_integer(),
          committed_layout=""
         }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

gen_mbox(#state{endpoint=Endpoint, reg_names=RegNames}) ->
    noshrink( ?LET(RegName, oneof(RegNames),
                   {RegName, endpoint2nodename(Endpoint)} )).

gen_rank() ->
    choose(1, 100).

gen_rank(#state{last_rank=0}) ->
    gen_rank();
gen_rank(#state{last_rank=LR}) ->
    frequency([{10, LR},
               { 2, gen_rank()}]).

gen_layout() ->
    "{\n  \"layoutServers\": [\n    \"localhost:8000\"\n  ],\n  \"sequencers\": [\n    \"localhost:8000\"\n  ],\n  \"segments\": [\n    {\n      \"replicationMode\": \"CHAIN_REPLICATION\",\n      \"start\": 0,\n      \"end\": -1,\n      \"stripes\": [\n        {\n          \"logServers\": [\n            \"localhost:8000\"\n          ]\n        }\n      ]\n    }\n  ],\n  \"epoch\": 1\n}".

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

initial_state() ->
    initial_state(local_mboxes(), local_endpoint()).

initial_state(Mboxes, Endpoint) ->
    #state{endpoint=Endpoint, reg_names=Mboxes}.

precondition(S, {call,_,reset,_}) ->
    not S#state.reset_p;
precondition(S, {call,_,commit,[_,_,_,Layout]}) ->
    S#state.reset_p andalso Layout /= "";
precondition(S, _Call) ->
    S#state.reset_p.

command(S=#state{endpoint=Endpoint, reset_p=false}) ->
    {call, ?MODULE, reset, [gen_mbox(S), Endpoint]};
command(S=#state{endpoint=Endpoint, reset_p=true,
                 proposed_layout=ProposedLayout}) ->
    frequency(
      [
       %% {5,  {call, ?MODULE, resetAMNESIA, [gen_mbox(S), Endpoint]}},
       {5,  {call, ?MODULE, reboot, [gen_mbox(S), Endpoint]}},
       {20, {call, ?MODULE, query, [gen_mbox(S), Endpoint]}},
       {20, {call, ?MODULE, prepare, [gen_mbox(S), Endpoint, gen_rank()]}},
       {20, {call, ?MODULE, propose, [gen_mbox(S), Endpoint, gen_rank(S), gen_layout()]}},
       {20, {call, ?MODULE, commit, [gen_mbox(S), Endpoint, gen_rank(S), ProposedLayout]}}
      ]).

postcondition(_S, {call,_,RRR,[_Mbox, _EP]}, Ret)
  when RRR == reboot; RRR == reset; RRR == resetAMNESIA ->
    case Ret of
        ["OK"] -> true;
        Else   -> {got, Else}
    end;
postcondition(#state{}, {call,_,query,[_Mbox, _EP]}, Ret) ->
    case Ret of
        timeout ->
            false;
        ["OK", _JSON] ->
            true;
        Else ->
            io:format(user, "Q ~p\n", [Else]),
            false
    end;
postcondition(#state{last_rank=LastRank},
              {call,_,prepare,[_Mbox, _EP, Rank]}, RetStr) ->
    case termify(RetStr) of
        ok ->
            Rank > LastRank;
        {error, outrankedException} ->
            Rank =< LastRank;
        Else ->
            {prepare, Rank, last_rank, LastRank, Else}
    end;
postcondition(#state{last_rank=LastRank, proposed_ok_rank=ProposedOkRank},
              {call,_,propose,[_Mbox, _EP, Rank, _Layout]}, RetStr) ->
    case termify(RetStr) of
        ok ->
            Rank == LastRank andalso Rank > ProposedOkRank;
        {error, outrankedException} ->
            Rank == ProposedOkRank   %% 2nd propose at same rank is error
            orelse
            Rank /= LastRank;
        Else ->
            {propose, Rank, last_rank, LastRank, Else}
    end;
postcondition(#state{last_rank=LastRank, proposed_ok_rank=ProposedOkRank},
              {call,_,commit,[_Mbox, _EP, Rank, _Layout]}, RetStr) ->
    case termify(RetStr) of
        ok ->
            Rank == LastRank andalso Rank == ProposedOkRank;
        {error, outrankedException} ->
            not (Rank == LastRank andalso Rank == ProposedOkRank);
        Else ->
            {commit, Rank, last_rank, LastRank,
             proposed_ok_rank, ProposedOkRank, Else}
    end.

next_state(S, _V, {call,_,reset,[_Svr, _Str]}) ->
    S#state{reset_p=true};
next_state(S, _V, {call,_,resetAMNESIA,[_Svr, _Str]}) ->
    S#state{reset_p=true};
next_state(S=#state{last_rank=LastRank}, _V,
           {call,_,prepare,[_Mbox, _EP, Rank]}) ->
    if Rank > LastRank ->
            S#state{last_rank=Rank};
       true ->
            S
    end;
next_state(S=#state{last_rank=LastRank}, _V,
           {call,_,propose,[_Mbox, _EP, Rank, Layout]}) ->
    if Rank == LastRank ->
            S#state{proposed_ok_rank=Rank, proposed_layout=Layout};
       true ->
            S
    end;
next_state(S=#state{last_rank=LastRank, proposed_ok_rank=ProposedOkRank}, _V,
           {call,_,commit,[_Mbox, _EP, Rank, Layout]}) ->
    if Rank == LastRank andalso Rank == ProposedOkRank ->
            S#state{committed_rank=Rank, committed_layout=Layout};
       true ->
            S
    end;
next_state(S, _V, _NoSideEffectCall) ->
    S.

%%%%

reset(Mbox, Endpoint) ->
    io:format(user, "R", []),
    java_rpc(Mbox, reset, Endpoint).

resetAMNESIA(Mbox, Endpoint) ->
    reset(Mbox, Endpoint).

reboot(Mbox, Endpoint) ->
    io:format(user, "r", []),
    java_rpc(Mbox, reboot, Endpoint).

query(Mbox, Endpoint) ->
    java_rpc(Mbox, "query", Endpoint, []).

prepare(Mbox, Endpoint, Rank) ->
    java_rpc(Mbox, "prepare", Endpoint, ["-r", integer_to_list(Rank)]).

propose(Mbox, Endpoint, Rank, Layout) ->
    TmpPath = lists:flatten(io_lib:format("/tmp/layout.~w", [now()])),
    ok = file:write_file(TmpPath, Layout),
    Res = java_rpc(Mbox, "propose", Endpoint, ["-r", integer_to_list(Rank),
                                               "-l", TmpPath]),
    file:delete(TmpPath),
    Res.

commit(Mbox, Endpoint, Rank, Layout) ->
    TmpPath = lists:flatten(io_lib:format("/tmp/layout.~w", [now()])),
    ok = file:write_file(TmpPath, Layout),
    Res = java_rpc(Mbox, "committed", Endpoint, ["-r", integer_to_list(Rank),
                                                 "-l", TmpPath]),
    file:delete(TmpPath),
    Res.

termify(["OK"]) ->
    ok;
termify(["ERROR", "Exception during prepare" ++ _E1, E2|_]) ->
    case string:str(E2, "OutrankedException") of
        I when I >= 0 ->
            {error, outrankedException}
    end;
termify(["ERROR", "Exception during propose" ++ _E1, E2|_]) ->
    case string:str(E2, "OutrankedException") of
        I when I >= 0 ->
            {error, outrankedException}
    end;
termify(["ERROR", "Exception during commit" ++ _E1, E2|_]) ->
    case string:str(E2, "OutrankedException") of
        I when I >= 0 ->
            {error, outrankedException}
    end;
termify(timeout) ->
    timeout.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

local_mboxes() ->
    [cmdlet0, cmdlet1, cmdlet2, cmdlet3, cmdlet4,
     cmdlet5, cmdlet6, cmdlet7, cmdlet8, cmdlet9].

local_endpoint() ->
    "sbb5:8000".

endpoint2nodename(Endpoint) ->
    [HostName, Port] = string:tokens(Endpoint, ":"),
    list_to_atom("corfu-" ++ Port ++ "@" ++ HostName).

prop() ->
    prop(1).

prop(MoreCmds) ->
    prop(MoreCmds, local_mboxes(), local_endpoint()).

prop(MoreCmds, Mboxes, Endpoint) ->
    random:seed(now()),
    %% Hmmmm, more_commands() doesn't appear to work correctly with Proper.
    ?FORALL(Cmds, more_commands(MoreCmds,
                                commands(?MODULE,
                                         initial_state(Mboxes, Endpoint))),
            begin
                {H,S,Res} = run_commands(?MODULE, Cmds),
                ?WHENFAIL(
                io:format("H: ~p~nS: ~w~nR: ~p~n", [H,S,Res]),
                aggregate(command_names(Cmds),
                collect(length(Cmds) div 10,
                        Res == ok)))
            end).

prop_parallel() ->
    prop_parallel(1).

prop_parallel(MoreCmds) ->
    prop_parallel(MoreCmds, local_mboxes(), local_endpoint()).

% % EQC has an exponential worst case for checking {SIGH}
-define(PAR_CMDS_LIMIT, 6). % worst case so far @ 7 = 52 seconds!

prop_parallel(MoreCmds, Mboxes, Endpoint) ->
    random:seed(now()),
    %% Drat.  EQC 1.37.2's more_commands() is broken: the parallel
    %% commands lists aren't resized.  So, we're going to do it
    %% ourself, bleh.
    ?FORALL(_NumPars,
            choose(1, 4), %% ?NUM_LOCALHOST_CMDLETS - 3),
    ?FORALL(NewCs,
            %% [more_commands(MoreCmds,
            %%                non_empty(
            %%                  commands(?MODULE,
            %%                           initial_state(Mboxes, Endpoint)))) ||
            %%     _ <- lists:seq(1, NumPars)],
            more_commands(MoreCmds,
                           non_empty(
                             parallel_commands(?MODULE,
                                      initial_state(Mboxes, Endpoint)))),
            begin
                %% [SeqList|_] = hd(NewCs),
                %% Cmds = {SeqList,
                %%         lists:map(fun(L) -> lists:sublist(seq_to_par_cmds(L),
                %%                                           ?PAR_CMDS_LIMIT) end,
                %%                   tl(NewCs))},
                Cmds = NewCs,
                {Seq, Pars} = Cmds,
                Len = length(Seq) +
                    lists:foldl(fun(L, Acc) -> Acc + length(L) end, 0, Pars),
                {Elapsed, {H,Hs,Res}} = timer:tc(fun() -> run_parallel_commands(?MODULE, Cmds) end),
                if Elapsed > 2*1000*1000 ->
                        io:format(user, "~w,~w", [length(Seq), lists:map(fun(L) -> length(L) end, Pars) ]),
                        io:format(user, "=~w sec,", [Elapsed / 1000000]);
                   true ->
                        ok
                end,
                ?WHENFAIL(
                io:format("H: ~p~nHs: ~p~nR: ~p~n", [H,Hs,Res]),
                %% pretty_commands(?MODULE, Cmds, {H,Hs,Res},
                aggregate(command_names(Cmds),
                collect(if Len == 0 -> 0;
                           true     -> (Len div 10) + 1
                        end,
                        Res == ok)))
            end)).

seq_to_par_cmds(L) ->
    [Cmd || Cmd <- L,
            element(1, Cmd) /= init].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

java_rpc(Node, reset, Endpoint) ->
    AllArgs = ["corfu_layout", "reset", Endpoint],
    java_rpc_call(Node, AllArgs);
java_rpc(Node, reboot, Endpoint) ->
    AllArgs = ["corfu_layout", "reboot", Endpoint],
    java_rpc_call(Node, AllArgs).

java_rpc({_RegName, _NodeName} = Mbox, CmdName, Endpoint, Args) ->
    AllArgs = ["corfu_layout", CmdName, Endpoint] ++ Args,
    java_rpc_call(Mbox, AllArgs).

java_rpc_call(Mbox, AllArgs) ->
    ID = make_ref(),
    Mbox ! {self(), ID, AllArgs},
    receive
        {ID, Res} ->
            Res
    after 2*1000 ->
            timeout
    end.
