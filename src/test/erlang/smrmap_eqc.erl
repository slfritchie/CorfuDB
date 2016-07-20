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
    oneof([[choose($a, $b)],                     % make it a list
           [choose($a, $z)]]).                   % make it a list

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
    io:format(user, "p", []),
    java_rpc(Stream, ["put", Key ++ "," ++ Val]).

put_post(#state{d=D}, [_Str, Key, _Val], Ret) ->
    case Ret of
        timeout ->
            false;
        ["OK", Prev] ->
            case orddict:find(Key, D) of
                error                  -> Prev == [];
                {ok, V} when V == Prev -> true;
                {ok, Else}             -> {key, Key, exp, Else, got, Prev}
            end
    end.

put_next(S=#state{d=D}, _V, [_Str, Key, Val]) ->
    S#state{d=orddict:store(Key, Val, D)}.

get_args(#state{stream=Stream}) ->
    [Stream, gen_key()].

get(Stream, Key) ->
    io:format(user, "g", []),
    java_rpc(Stream, ["get", Key]).

get_post(S, [Str, Key], Ret) ->
    %% get's return value is the same as post's return value, so
    %% mock up a put call and share put_post().
    put_post(S, [Str, Key, <<"mock val from get_post()">>], Ret).

get_next(S, _V, _Args) ->
    S.

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
    after 2*1000 ->
            timeout
    end.

% -endif. % EQC
