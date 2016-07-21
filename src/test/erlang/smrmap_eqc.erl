-module(smrmap_eqc).

%% To compile and run with Quviq's QuickCheck:
%%
%% $ erl -sname foo -pz ~/lib/eqc/ebin
%%
%% > c(smrmap_eqc, [{d, 'EQC'}]).
%% > eqc:quickcheck(smrmap_eqc:prop()).
%%
%% To compile and run with Proper:
%%
%% $ erl -sname foo -pz /Users/fritchie/src/erlang/proper/ebin
%%
%% > c(smrmap_eqc, [{d, 'PROPER'}]).
%% > proper:quickcheck(smrmap_eqc:prop()).

-ifdef(PROPER).
-include_lib("proper/include/proper.hrl").
-endif.

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-endif.

%% -include_lib("eunit/include/eunit.hrl").

-compile(export_all).

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
    #state{stream=random:uniform(999*999)}.
    %% #state{stream=42}.

precondition(S, {call,_,reset,_}) ->
    not S#state.reset_p;
precondition(S, _Call) ->
    S#state.reset_p.

command(#state{stream=Stream, reset_p=false}) ->
    {call, ?MODULE, reset, [Stream]};
command(#state{stream=Stream, reset_p=true}) ->
    frequency([
               {20, {call, ?MODULE, put, [Stream, gen_key(), gen_val()]}},
               { 5, {call, ?MODULE, get, [Stream, gen_key()]}},
               { 3, {call, ?MODULE, size, [Stream]}},
               { 3, {call, ?MODULE, isEmpty, [Stream]}},
               { 3, {call, ?MODULE, containsKey, [Stream, gen_key()]}},
               %% BOO.  Our ASCII-oriented protocol can't tell the difference
               %% between an arity 0 function and an arity 1 function with
               %% an argument of length 0.
               { 3, {call, ?MODULE, containsValue, [Stream, non_empty(
                                                              gen_val())]}},
               { 5, {call, ?MODULE, remove, [Stream, gen_key()]}},
               { 3, {call, ?MODULE, clear, [Stream]}},
               { 3, {call, ?MODULE, keySet, [Stream]}},
               { 3, {call, ?MODULE, values, [Stream]}},
               { 3, {call, ?MODULE, entrySet, [Stream]}}
              ]).

postcondition(_S, {call,_,reset,[_Str]}, Ret) ->
    case Ret of
        ["OK"] -> true;
        Else   -> {got, Else}
    end;
postcondition(#state{d=D}, {call,_,put,[_Str, Key, _Val]}, Ret) ->
    case Ret of
        timeout ->
            false;
        ["OK", Prev] ->
            case orddict:find(Key, D) of
                error                  -> Prev == [];
                {ok, V} when V == Prev -> true;
                {ok, Else}             -> {key, Key, expected, Else, got, Prev}
            end
    end;
postcondition(S, {call,_,get,[Str, Key]}, Ret) ->
    %% get's return value is the same as post's return value, so
    %% mock up a put call and share put_post().
    postcondition(S, {call,x,put,[Str, Key, <<"get_post()">>]}, Ret);
postcondition(#state{d=D}, {call,_,size,[_Stream]}, Res) ->
    case Res of
        ["OK", SizeStr] ->
            list_to_integer(SizeStr) == length(orddict:to_list(D));
        Else ->
            {got, Else}
    end;
postcondition(#state{d=D}, {call,_,isEmpty,[_Stream]}, Res) ->
    case Res of
        ["OK", Bool] ->
            list_to_atom(Bool) == orddict:is_empty(D);
        Else ->
            {got, Else}
    end;
postcondition(#state{d=D}, {call,_,containsKey,[_Stream, Key]}, Res) ->
    case Res of
        ["OK", Bool] ->
            list_to_atom(Bool) == orddict:is_key(Key, D);
        Else ->
            {got, Else}
    end;
postcondition(#state{d=D}, {call,_,containsValue,[_Stream, Value]}, Res) ->
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
    end;
postcondition(S, {call,_,remove,[Str, Key]}, Ret) ->
    %% remove's return value is the same as post's return value, so
    %% mock up a put call and share put_post().
    postcondition(S, {call,x,put,[Str, Key, <<"remove_post()">>]}, Ret);
postcondition(_S, {call,_,clear,[_Str]}, ["OK", []]) ->
    true;
postcondition(#state{d=D}, {call,_,keySet,[_Str]}, Ret) ->
    case Ret of
        ["OK", X] ->
            X2 = string:strip(string:strip(X, left, $[), right, $]),
            Ks = string:tokens(X2, ", "),
            lists:sort(Ks) == lists:sort([K || {K,_V} <- orddict:to_list(D)])
    end;
postcondition(#state{d=D}, {call,_,values,[_Str]}, Ret) ->
    case Ret of
        ["OK", X] ->
            X2 = string:strip(string:strip(X, left, $[), right, $]),
            Vs = string:tokens(X2, ", "),
            %% BOO.  Our ASCII protocol can't tell us the difference between
            %% an empty list and a list of length one that contains an
            %% empty string.
            lists:sort(Vs) == lists:sort([V || {_K,V} <- orddict:to_list(D),
                                               V /= ""])
    end;
postcondition(#state{d=D}, {call,_,entrySet,[_Str]}, Ret) ->
    case Ret of
        ["OK", X] ->
            X2 = string:strip(string:strip(X, left, $[), right, $]),
            Ps = string:tokens(X2, ", "),
            KVs = [begin
                       case string:tokens(Pair, "=") of
                           [K, V] -> {K, V};
                           [K]    -> {K, ""}
                       end
                   end || Pair <- Ps],
            lists:sort(KVs) == lists:sort(orddict:to_list(D))
    end.

next_state(S, _V, {call,_,reset,[_Str]}) ->
    S#state{reset_p=true};
next_state(S=#state{d=D}, _V, {call,_,put,[_Str, Key, Val]}) ->
    S#state{d=orddict:store(Key, Val, D)};
next_state(S=#state{d=D}, _V, {call,_,remove,[_Str, Key]}) ->
    S#state{d=orddict:erase(Key, D)};
next_state(S, _V, {call,_,clear,[_Str]}) ->
    S#state{d=orddict:new()};
next_state(S, _V, _NoSideEffectCall) ->
    S.

%%%%

reset(Stream) ->
    java_rpc(reset, Stream).

put(Stream, Key, Val) ->
    java_rpc(Stream, ["put", Key ++ "," ++ Val]).

get(Stream, Key) ->
    java_rpc(Stream, ["get", Key]).

size(Stream) ->
    java_rpc(Stream, ["size"]).

isEmpty(Stream) ->
    java_rpc(Stream, ["isEmpty"]).

containsKey(Stream, Key) ->
    java_rpc(Stream, ["containsKey", Key]).

containsValue(Stream, Value) ->
    java_rpc(Stream, ["containsValue", Value]).

remove(Stream, Key) ->
    java_rpc(Stream, ["remove", Key]).

%% %% putAll() can't be tested because our ASCII protocol can't represent
%% %% the needed map.

clear(Stream) ->
    java_rpc(Stream, ["clear"]).

keySet(Stream) ->
    java_rpc(Stream, ["keySet"]).

values(Stream) ->
    java_rpc(Stream, ["values"]).

entrySet(Stream) ->
    java_rpc(Stream, ["entrySet"]).

prop() ->
    prop(1).

%% Hmmmm, more_commands() doesn't appear to work correctly with Proper.

prop(MoreCmds) ->
    random:seed(now()),
    ?FORALL(Cmds, more_commands(MoreCmds, commands(?MODULE)),
            begin
                {H,S,Res} = run_commands(?MODULE, Cmds),
                %% ["OK", []] = clear(S#state.stream),
                ?WHENFAIL(
                io:format("H: ~p~nS: ~w~nR: ~w~n", [H,S,Res]),
                aggregate(command_names(Cmds),
                collect(length(Cmds) div 10,
                        Res == ok)))
            end).

prop_parallel() ->
    prop_parallel(1).

prop_parallel(MoreCmds) ->
    random:seed(now()),
    ?FORALL(Cmds, more_commands(MoreCmds, parallel_commands(?MODULE)),
            begin
                %% io:format(user, "Cmds ~p\n", [Cmds]),
                {H,Hs,Res} = run_parallel_commands(?MODULE, Cmds),
                {Seq, Pars} = Cmds,
                Len = length(Seq) +
                    lists:foldl(fun(L, Acc) -> Acc + length(L) end, 0, Pars),
                ?WHENFAIL(
                io:format("H: ~p~nHs: ~p~nR: ~w~n", [H,Hs,Res]),
                aggregate(command_names(Cmds),
                collect(if Len == 0 -> 0;
                           true     -> (Len div 10) + 1
                        end,
                        Res == ok)))
                %% pretty_commands(client, Cmds, {H, Hs, Res},
                %%                 Res == ok)))
            end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

java_rpc(reset, Stream) ->
    clear(Stream),
    AllArgs = ["corfu_smrobject", "reset"],
    java_rpc_call(AllArgs);
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

