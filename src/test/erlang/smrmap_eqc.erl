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
    oneof(["",
           "Hello-world!",                      % no spaces or commas!
           "Another-value",
           ?LET(L, choose(0, 50),
                vector(L, choose($a, $z)))]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

initial_state() ->
    %% #state{stream=random:uniform(999*999)}.
    #state{stream=42}.

precondition_common(S) ->
    S#state.reset_p.

weight(_S, put) ->
    50;
weight(_S, clear) ->
    3;
weight(_S, _) ->
    10.

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

size_args(#state{stream=Stream}) ->
    [Stream].

size(Stream) ->
    java_rpc(Stream, ["size"]).

size_post(#state{d=D}, [_Stream], Res) ->
    case Res of
        ["OK", SizeStr] ->
            list_to_integer(SizeStr) == length(orddict:to_list(D));
        Else ->
            {got, Else}
    end.

size_next(S, _V, _Args) ->
    S.

isEmpty_args(#state{stream=Stream}) ->
    [Stream].

isEmpty(Stream) ->
    java_rpc(Stream, ["isEmpty"]).

isEmpty_post(#state{d=D}, [_Stream], Res) ->
    case Res of
        ["OK", Bool] ->
            list_to_atom(Bool) == orddict:is_empty(D);
        Else ->
            {got, Else}
    end.

isEmpty_next(S, _V, _Args) ->
    S.

containsKey_args(#state{stream=Stream}) ->
    [Stream, gen_key()].

containsKey(Stream, Key) ->
    java_rpc(Stream, ["containsKey", Key]).

containsKey_post(#state{d=D}, [_Stream, Key], Res) ->
    case Res of
        ["OK", Bool] ->
            list_to_atom(Bool) == orddict:is_key(Key, D);
        Else ->
            {got, Else}
    end.

containsKey_next(S, _V, _Args) ->
    S.

containsValue_args(#state{stream=Stream}) ->
    %% BOO.  Our ASCII-oriented protocol can't tell the difference
    %% between an arity 0 function and an arity 1 function with
    %% an argument of length 0.
    [Stream, non_empty(gen_val())].

containsValue(Stream, Value) ->
    java_rpc(Stream, ["containsValue", Value]).

containsValue_post(#state{d=D}, [_Stream, Value], Res) ->
    io:format(user, "c", []),
    case Res of
        ["OK", Bool] ->
            Val_in_d = case [V || {_K, V} <- orddict:to_list(D),
                                  V == Value] of
                           [] -> false;
                           _  -> true
                       end,
            list_to_atom(Bool) == Val_in_d;
        Else ->
            {got, Else}
    end.

containsValue_next(S, _V, _Args) ->
    S.

remove_args(#state{stream=Stream}) ->
    [Stream, gen_key()].

remove(Stream, Key) ->
    io:format(user, "e", []),
    java_rpc(Stream, ["remove", Key]).

remove_post(S, [Str, Key], Ret) ->
    %% remove's return value is the same as post's return value, so
    %% mock up a put call and share put_post().
    put_post(S, [Str, Key, <<"mock val from remove_post()">>], Ret).

remove_next(S=#state{d=D}, _V, [_Str, Key]) ->
    S#state{d=orddict:erase(Key, D)}.

%% putAll() can't be tested because our ASCII protocol can't represent
%% the needed map.

clear_args(#state{stream=Stream}) ->
    [Stream].

clear(Stream) ->
    io:format(user, "*", []),
    java_rpc(Stream, ["clear"]).

clear_post(_S, [_Str], ["OK", []]) ->
    true.

clear_next(S, _V, [_Str]) ->
    S#state{d=orddict:new()}.

keySet_args(#state{stream=Stream}) ->
    [Stream].

keySet(Stream) ->
    io:format(user, "K", []),
    java_rpc(Stream, ["keySet"]).

keySet_post(#state{d=D}, [_Str], Ret) ->
    case Ret of
        ["OK", X] ->
            X2 = string:strip(string:strip(X, left, $[), right, $]),
            Ks = string:tokens(X2, ", "),
            lists:sort(Ks) == lists:sort([K || {K,_V} <- orddict:to_list(D)])
    end.

keySet_next(S, _V, _Args) ->
    S.

values_args(#state{stream=Stream}) ->
    [Stream].

values(Stream) ->
    io:format(user, "V", []),
    java_rpc(Stream, ["values"]).

values_post(#state{d=D}, [_Str], Ret) ->
    case Ret of
        ["OK", X] ->
            X2 = string:strip(string:strip(X, left, $[), right, $]),
            Vs = string:tokens(X2, ", "),
            lists:sort(Vs) == lists:sort([V || {_K,V} <- orddict:to_list(D)])
    end.

values_next(S, _V, _Args) ->
    S.

entrySet_args(#state{stream=Stream}) ->
    [Stream].

entrySet(Stream) ->
    io:format(user, "V", []),
    java_rpc(Stream, ["entrySet"]).

entrySet_post(#state{d=D}, [_Str], Ret) ->
    case Ret of
        ["OK", X] ->
            X2 = string:strip(string:strip(X, left, $[), right, $]),
            Ps = string:tokens(X2, ", "),
            KVs = [begin
                       [K, V] = string:tokens(Pair, "="),
                       {K, V}
                   end || Pair <- Ps],
            lists:sort(KVs) == lists:sort(orddict:to_list(D))
    end.

entrySet_next(S, _V, _Args) ->
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
