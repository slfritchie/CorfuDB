-module(smrmap_qc).

%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 VMware, Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% See the README.md file for instructions for compiling & running.

-ifdef(PROPER).
%% Automagically import generator functions like choose(), frequency(), etc.
-include_lib("proper/include/proper.hrl").
-endif.

-ifdef(EQC).
%% Automagically import generator functions like choose(), frequency(), etc.
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-endif.

-define(TIMEOUT, 2*1000).

-compile(export_all).

-record(state, {
          endpoint :: string(),
          reg_names :: list(),
          reset_p = false :: boolean(),
          stream :: non_neg_integer(),
          d=orddict:new() :: orddict:orddict(),
          server_list=[] :: list()
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

gen_svr(#state{server_list=Svrs}) ->
    noshrink(oneof(Svrs)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

initial_state() ->
    initial_state(qc_java:local_mboxes(), qc_java:local_endpoint()).

initial_state(Mboxes, Endpoint) ->
    #state{endpoint=Endpoint, reg_names=Mboxes,
           stream=42}.  %% #state{stream=random:uniform(999*999)}

precondition(S, {call,_,reset,_}) ->
    not S#state.reset_p;
precondition(S, _Call) ->
    S#state.reset_p.

command(S=#state{stream=Stream, reset_p=false}) ->
    {call, ?MODULE, reset, [gen_svr(S), Stream]};
command(S=#state{stream=Stream, reset_p=true}) ->
    frequency(
      [
       {20, {call, ?MODULE, put, [gen_svr(S), Stream, gen_key(), gen_val()]}},
       { 5, {call, ?MODULE, get, [gen_svr(S), Stream, gen_key()]}},
       { 3, {call, ?MODULE, size, [gen_svr(S), Stream]}},
       { 3, {call, ?MODULE, isEmpty, [gen_svr(S), Stream]}},
       { 3, {call, ?MODULE, containsKey, [gen_svr(S), Stream, gen_key()]}},
       %% BOO.  Our ASCII-oriented protocol can't tell the difference
       %% between an arity 0 function and an arity 1 function with
       %% an argument of length 0.
       { 3, {call, ?MODULE, containsValue, [gen_svr(S),
                                            Stream, non_empty(gen_val())]}},
       { 5, {call, ?MODULE, remove, [gen_svr(S), Stream, gen_key()]}},
       { 3, {call, ?MODULE, clear, [gen_svr(S), Stream]}},
       { 3, {call, ?MODULE, keySet, [gen_svr(S), Stream]}},
       { 3, {call, ?MODULE, values, [gen_svr(S), Stream]}},
       { 3, {call, ?MODULE, entrySet, [gen_svr(S), Stream]}}
      ]).

postcondition(_S, {call,_,reset,[_Svr, _Str]}, Ret) ->
    case Ret of
        ["OK"] -> true;
        Else   -> {got, Else}
    end;
postcondition(#state{d=D}, {call,_,put,[_Svr, _Str, Key, _Val]}, Ret) ->
    case Ret of
        timeout ->
            false;
        ["OK"] ->
            orddict:find(Key, D) == error;
        ["OK", Prev] ->
            case orddict:find(Key, D) of
                error                  -> Prev == [];
                {ok, V} when V == Prev -> true;
                {ok, Else}             -> {key, Key, expected, Else, got, Prev}
            end
    end;
postcondition(S, {call,_,get,[_Svr, Str, Key]}, Ret) ->
    %% get's return value is the same as post's return value, so
    %% mock up a put call and share put_post().
    postcondition(S, {call,x,put,[_Svr, Str, Key, <<"get_post()">>]}, Ret);
postcondition(#state{d=D}, {call,_,size,[_Svr, _Stream]}, Res) ->
    case Res of
        ["OK", SizeStr] ->
            list_to_integer(SizeStr) == length(orddict:to_list(D));
        Else ->
            {got, Else}
    end;
postcondition(#state{d=D}, {call,_,isEmpty,[_Svr, _Stream]}, Res) ->
    case Res of
        ["OK", Bool] ->
            list_to_atom(Bool) == orddict:is_empty(D);
        Else ->
            {got, Else}
    end;
postcondition(#state{d=D}, {call,_,containsKey,[_Svr, _Stream, Key]}, Res) ->
    case Res of
        ["OK", Bool] ->
            list_to_atom(Bool) == orddict:is_key(Key, D);
        Else ->
            {got, Else}
    end;
postcondition(#state{d=D}, {call,_,containsValue,[_Svr, _Stream, Value]}, Res) ->
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
postcondition(S, {call,_,remove,[_Svr, Str, Key]}, Ret) ->
    %% remove's return value is the same as post's return value, so
    %% mock up a put call and share put_post().
    postcondition(S, {call,x,put,[_Svr, Str, Key, <<"remove_post()">>]}, Ret);
postcondition(_S, {call,_,clear,[_Svr, _Str]}, ["OK"]) ->
    true;
postcondition(#state{d=D}, {call,_,keySet,[_Svr, _Str]}, Ret) ->
    case Ret of
        ["OK", X] ->
            X2 = string:strip(string:strip(X, left, $[), right, $]),
            Ks = string:tokens(X2, ", "),
            lists:sort(Ks) == lists:sort([K || {K,_V} <- orddict:to_list(D)])
    end;
postcondition(#state{d=D}, {call,_,values,[_Svr, _Str]}, Ret) ->
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
postcondition(#state{d=D}, {call,_,entrySet,[_Svr, _Str]}, Ret) ->
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

next_state(S, _V, {call,_,reset,[_Svr, _Str]}) ->
    S#state{reset_p=true};
next_state(S=#state{d=D}, _V, {call,_,put,[_Svr, _Str, Key, Val]}) ->
    S#state{d=orddict:store(Key, Val, D)};
next_state(S=#state{d=D}, _V, {call,_,remove,[_Svr, _Str, Key]}) ->
    S#state{d=orddict:erase(Key, D)};
next_state(S, _V, {call,_,clear,[_Svr, _Str]}) ->
    S#state{d=orddict:new()};
next_state(S, _V, _NoSideEffectCall) ->
    S.

%%%%

reset(Mbox, Endpoint) ->
    io:format(user, "R", []),
    rpc(Mbox, reset, Endpoint).

reboot(Mbox, Endpoint) ->
    io:format(user, "r", []),
    rpc(Mbox, reboot, Endpoint).

put(Mbox, Endpoint, Stream, Key, Val) ->
    rpc(Mbox, Endpoint, Stream, ["put", Key ++ "," ++ Val]).

get(Mbox, Endpoint, Stream, Key) ->
    rpc(Mbox, Endpoint, Stream, ["get", Key]).

size(Mbox, Endpoint, Stream) ->
    rpc(Mbox, Endpoint, Stream, ["size"]).

isEmpty(Mbox, Endpoint, Stream) ->
    rpc(Mbox, Endpoint, Stream, ["isEmpty"]).

containsKey(Mbox, Endpoint, Stream, Key) ->
    rpc(Mbox, Endpoint, Stream, ["containsKey", Key]).

containsValue(Mbox, Endpoint, Stream, Value) ->
    rpc(Mbox, Endpoint, Stream, ["containsValue", Value]).

remove(Mbox, Endpoint, Stream, Key) ->
    rpc(Mbox, Endpoint, Stream, ["remove", Key]).

%% %% putAll() can't be tested because our ASCII protocol can't represent
%% %% the needed map.

clear(Mbox, Endpoint, Stream) ->
    rpc(Mbox, Endpoint, Stream, ["clear"]).

keySet(Mbox, Endpoint, Stream) ->
    rpc(Mbox, Endpoint, Stream, ["keySet"]).

values(Mbox, Endpoint, Stream) ->
    rpc(Mbox, Endpoint, Stream, ["values"]).

entrySet(Mbox, Endpoint, Stream) ->
    rpc(Mbox, Endpoint, Stream, ["entrySet"]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

prop() ->
    prop(1).

prop(MoreCmds) ->
    prop(MoreCmds, qc_java:local_mboxes(), qc_java:local_endpoint()).

prop(MoreCmds, Mboxes, Endpoint) ->
    random:seed(now()),
    %% Hmmmm, more_commands() doesn't appear to work correctly with Proper.
    ?FORALL(Cmds, more_commands(MoreCmds,
                                commands(?MODULE,
                                         initial_state(Mboxes, Endpoint))),
            qc_java:run_always(1, ?MODULE, Cmds,
                               fun(Mod, TheCmds) ->
                                       run_commands(Mod, TheCmds)
                               end,
                               fun(_TheCmds, _H, _S_or_Hs, Res) ->
                                       Res == ok
                               end)
           ).

prop_parallel() ->
    prop_parallel(1).

prop_parallel(MoreCmds) ->
    prop_parallel(MoreCmds, qc_java:local_mboxes(), qc_java:local_endpoint()).

prop_parallel(MoreCmds, Mboxes, Endpoint) ->
    random:seed(now()),
    ?FORALL(Cmds,
            more_commands(MoreCmds,
                           non_empty(
                             parallel_commands(?MODULE,
                                      initial_state(Mboxes, Endpoint)))),
            qc_java:run_always(100, ?MODULE, Cmds,
                               fun(Mod, TheCmds) ->
                                       run_parallel_commands(Mod, TheCmds)
                               end,
                               fun(_TheCmds, _H, _S_or_Hs, Res) ->
                                       Res == ok
                               end)
           ).

seq_to_par_cmds(L) ->
    [Cmd || Cmd <- L,
            element(1, Cmd) /= init,
            element(3, element(3, Cmd)) /= reset].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% rpc(Mbox, reset, Stream) ->
%%     clear(Mbox, Stream),
%%     AllArgs = ["corfu_smrobject", "reset"],
%%     java_rpc_call(Mbox, AllArgs);
%% rpc(Mbox, Stream, Args) ->
%%     StreamStr = integer_to_list(Stream),
%%     AllArgs = ["corfu_smrobject", "-c", "localhost:8000",
%%                "-s", StreamStr, "org.corfudb.runtime.collections.SMRMap"]
%%               ++ Args,
%%     qc_java:rpc_call(Mbox, AllArgs).

rpc(Mbox, reset, Endpoint) ->
    AllArgs = ["corfu_smrobject", "reset", Endpoint],
    qc_java:rpc_call(Mbox, AllArgs, ?TIMEOUT);
rpc(Mbox, reboot, Endpoint) ->
    AllArgs = ["corfu_smrobject", "reboot", Endpoint],
    qc_java:rpc_call(Mbox, AllArgs, ?TIMEOUT).

rpc({_RegName, _NodeName} = Mbox, Endpoint, Stream, Args) ->
    AllArgs = ["corfu_smrobject", "-c", Endpoint,
               "-s", Stream, "org.corfudb.runtime.collections.SMRMap"]
              ++ Args,
    qc_java:rpc_call(Mbox, AllArgs, ?TIMEOUT).
