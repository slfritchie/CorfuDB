-module(smrmap_eqc).

% -ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-define(TEST_TIME, 30).                      % seconds

-record(state, {
          reset_p = false :: boolean(),
          stream :: non_neg_integer(),
          d=orddict:new() :: orddict:orddict()
         }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

gen_key() ->
    [choose($a, $z)].                           % make it a list

gen_val() ->
    ?LET(L, choose(0, 50),
         vector(L, choose($a, $z))).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

initial_state() ->
    %% #state{stream=random:uniform(999*999)}.
    #state{stream=42}.

precondition_common(S) ->
    S#state.reset_p.

reset_pre(S) ->
    not S#state.reset_p.

reset_args(_) ->
    [].

reset() ->
    java_rpc(reset).

reset_post(#state{reset_p=false}, _Args, ["OK"]) ->
    true.

reset_next(S, _, _) ->
    S#state{reset_p=true}.

put_args(#state{stream=Stream}) ->
    [Stream, gen_key(), gen_val()].

put(Stream, Key, Val) ->
    java_rpc(Stream, ["put", Key ++ "," ++ Val]).

put_post(#state{d=D}, [_Str, Key, _Val], Ret) ->
    %% io:format(user, "put ~s <~s> -> ~p\n", [Key, Val, Ret]),
    case Ret of
        timeout ->
            false;
        ["OK", Prev=[]] ->
            case orddict:find(Key, D) of
                error                  -> true;
                {ok, V} when V == Prev -> true;
                {ok, Else}             -> {key, Key, exp, Else, got, Prev}
            end;
        ["OK", Previous] ->
            {ok, Previous} = orddict:find(Key, D),
            true
    end.

put_next(S=#state{d=D}, _V, [_Str, Key, Val]) ->
    S#state{d=orddict:store(Key, Val, D)}.

prop() ->
    random:seed(now()),
    ?FORALL(Cmds, commands(?MODULE),
            begin
                {H,S,Res} = run_commands(?MODULE, Cmds),
                ["OK", []] = java_rpc(S#state.stream, ["clear"]),
                pretty_commands(client, Cmds, {H, S, Res},
                                Res == ok)
            end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

java_rpc(reset) ->
    AllArgs = ["corfu_smrobject", "reset"],
    java_rpc_call(AllArgs).

java_rpc(Stream, Args) ->
    StreamStr = integer_to_list(Stream),
    AllArgs = ["corfu_smrobject", "-c", "localhost:8000",
               "-s", StreamStr, "org.corfudb.runtime.collections.SMRMap"]
              ++ Args,
    java_rpc_call(AllArgs).

java_rpc_call(AllArgs) ->
    ID = make_ref(),
    {cmdlet, 'corfu@sbb5'} ! {self(), ID, AllArgs},
    receive
        {ID, Res} ->
            Res
    after 5000 ->
            timeout
    end.

% -endif. % EQC
