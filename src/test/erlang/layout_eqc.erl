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

-ifdef(PROPER).
-include_lib("proper/include/proper.hrl").
-endif.

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-endif.

-compile(export_all).

-record(state, {
          endpoint :: string(),
          reg_names :: list()
         }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

gen_mbox(#state{endpoint=Endpoint, reg_names=RegNames}) ->
    noshrink( ?LET(RegName, oneof(RegNames),
                   {RegName, endpoint2nodename(Endpoint)} )).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

initial_state() ->
    initial_state(local_mboxes(), local_endpoint()).

initial_state(Mboxes, Endpoint) ->
    #state{endpoint=Endpoint, reg_names=Mboxes}.

precondition(_S, _Call) ->
    true.

command(S=#state{endpoint=Endpoint}) ->
    frequency(
      [
       {20, {call, ?MODULE, query, [gen_mbox(S), Endpoint]}}
      ]).

postcondition(#state{}, {call,_,query,[_Mbox, _EP]}, Ret) ->
    case Ret of
        timeout ->
            false;
        ["OK", JSON] ->
            true;
        Else ->
            io:format(user, "Q ~p\n", [Else]),
            false
    end.

next_state(S=#state{}, _V, {call,_,query,[_Mbox, _EP]}) ->
    S;
next_state(S, _V, _NoSideEffectCall) ->
    S.

%%%%

query(Mbox, Endpoint) ->
    java_rpc(Mbox, "query", Endpoint, []).

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

%% prop_parallel() ->
%%     prop_parallel(1).

%% prop_parallel(MoreCmds) ->
%%     prop_parallel(MoreCmds, local_servers()).

%% % % EQC has an exponential worst case for checking {SIGH}
%% -define(PAR_CMDS_LIMIT, 6). % worst case so far @ 7 = 52 seconds!

%% prop_parallel(MoreCmds, ServerList) ->
%%     random:seed(now()),
%%     %% Drat.  EQC 1.37.2's more_commands() is broken: the parallel
%%     %% commands lists aren't resized.  So, we're going to do it
%%     %% ourself, bleh.
%%     ?FORALL(NumPars,
%%             choose(1, 4), %% ?NUM_LOCALHOST_CMDLETS - 3),
%%     ?FORALL(NewCs,
%%             [more_commands(MoreCmds,
%%                            non_empty(
%%                              commands(?MODULE,
%%                                       initial_state(ServerList)))) ||
%%                 _ <- lists:seq(1, NumPars)],
%%             begin
%%                 [SeqList|_] = hd(NewCs),
%%                 Cmds = {SeqList,
%%                         lists:map(fun(L) -> lists:sublist(seq_to_par_cmds(L),
%%                                                           ?PAR_CMDS_LIMIT) end,
%%                                   tl(NewCs))},
%%                 {Seq, Pars} = Cmds,
%%                 Len = length(Seq) +
%%                     lists:foldl(fun(L, Acc) -> Acc + length(L) end, 0, Pars),
%%                 {Elapsed, {H,Hs,Res}} = timer:tc(fun() -> run_parallel_commands(?MODULE, Cmds) end),
%%                 if Elapsed > 2*1000*1000 ->
%%                         io:format(user, "~w,~w", [length(Seq), lists:map(fun(L) -> length(L) end, Pars) ]),
%%                         io:format(user, "=~w sec,", [Elapsed / 1000000]);
%%                    true ->
%%                         ok
%%                 end,
%%                 ?WHENFAIL(
%%                 io:format("H: ~p~nHs: ~p~nR: ~w~n", [H,Hs,Res]),
%%                 aggregate(command_names(Cmds),
%%                 collect(if Len == 0 -> 0;
%%                            true     -> (Len div 10) + 1
%%                         end,
%%                         Res == ok)))
%%             end)).

%% seq_to_par_cmds(L) ->
%%     [Cmd || Cmd <- L,
%%             element(1, Cmd) /= init].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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

