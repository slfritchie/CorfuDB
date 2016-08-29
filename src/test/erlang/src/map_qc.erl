-module(map_qc).

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

-include("qc_java.hrl").

-define(TIMEOUT, 12*1000).

-compile(export_all).

-record(state, {
          map_type = smrmap :: 'smrmap' | 'fgmap',
          endpoint :: string(),
          reg_names :: list(),
          reset_p = false :: boolean(),
          stream :: non_neg_integer(),
          d=orddict:new() :: orddict:orddict()
         }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

gen_mbox(#state{endpoint=Endpoint, reg_names=RegNames}) ->
    noshrink( ?LET(RegName, oneof(RegNames),
                   {RegName, qc_java:endpoint2nodename(Endpoint)} )).

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
    initial_state(smrmap, qc_java:local_mboxes(), qc_java:local_endpoint()).

initial_state(MapType, Mboxes, Endpoint) ->
    #state{map_type=MapType,
           endpoint=Endpoint, reg_names=Mboxes,
           stream=42}.  %% #state{stream=random:uniform(999*999)}

precondition(S, {call,_,reset,_}) ->
    not S#state.reset_p;
precondition(S, _Call) ->
    S#state.reset_p.

command(S=#state{endpoint=Endpoint, reset_p=false}) ->
    {call, ?MODULE, reset, [gen_mbox(S), Endpoint]};
command(S=#state{map_type=MapType,
                 endpoint=Endpoint, stream=Stream, reset_p=true}) ->
    frequency(
      [
       {20, {call, ?MODULE, put, [gen_mbox(S), Endpoint, Stream, MapType,
                                  gen_key(), gen_val()]}},
       { 5, {call, ?MODULE, get, [gen_mbox(S), Endpoint, Stream, MapType,
                                  gen_key()]}},
       { 3, {call, ?MODULE, size, [gen_mbox(S), Endpoint, Stream, MapType]}},
       { 3, {call, ?MODULE, isEmpty, [gen_mbox(S), Endpoint, Stream, MapType]}},
       { 3, {call, ?MODULE, containsKey, [gen_mbox(S), Endpoint, Stream, MapType,
                                          gen_key()]}},
       %% BOO.  Our ASCII-oriented protocol can't tell the difference
       %% between an arity 0 function and an arity 1 function with
       %% an argument of length 0.
       { 3, {call, ?MODULE, containsValue, [gen_mbox(S), Endpoint, Stream, MapType,
                                            non_empty(gen_val())]}},
       { 5, {call, ?MODULE, remove, [gen_mbox(S), Endpoint, Stream, MapType,
                                     gen_key()]}},
       { 3, {call, ?MODULE, clear, [gen_mbox(S), Endpoint, Stream, MapType]}},
       { 3, {call, ?MODULE, keySet, [gen_mbox(S), Endpoint, Stream, MapType]}},
       { 3, {call, ?MODULE, values, [gen_mbox(S), Endpoint, Stream, MapType]}},
       { 3, {call, ?MODULE, entrySet, [gen_mbox(S), Endpoint, Stream, MapType]}}
      ]).

postcondition(_S, {call,_,reset,[_Mbox, _EP]}, Ret) ->
    case Ret of
        ["OK"] -> true;
        Else   -> {got, Else}
    end;
postcondition(#state{d=D}, {call,_,put,[_Mbox, _EP, _Str, _MT, Key, _Val]}, Ret) ->
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
postcondition(S, {call,_,get,[_Mbox, _EP, Str, _MT, Key]}, Ret) ->
    %% get's return value is the same as post's return value, so
    %% mock up a put call and share put_post().
    postcondition(S, {call,x,put,[_Mbox, _EP, Str, _MT, Key, <<"get_post()">>]}, Ret);
postcondition(#state{d=D}, {call,_,size,[_Mbox, _EP, _Str, _MT]}, Res) ->
    case Res of
        ["OK", SizeStr] ->
            list_to_integer(SizeStr) == length(orddict:to_list(D));
        Else ->
            {got, Else}
    end;
postcondition(#state{d=D}, {call,_,isEmpty,[_Mbox, _EP, _Str, _MT]}, Res) ->
    case Res of
        ["OK", Bool] ->
            list_to_atom(Bool) == orddict:is_empty(D);
        Else ->
            {got, Else}
    end;
postcondition(#state{d=D}, {call,_,containsKey,[_Mbox, _EP, _Str, _MT, Key]}, Res) ->
    case Res of
        ["OK", Bool] ->
            list_to_atom(Bool) == orddict:is_key(Key, D);
        Else ->
            {got, Else}
    end;
postcondition(#state{d=D}, {call,_,containsValue,[_Mbox, _EP, _Str, _MT, Value]}, Res) ->
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
postcondition(S, {call,_,remove,[_Mbox, _EP, Str, _MT, Key]}, Ret) ->
    %% remove's return value is the same as post's return value, so
    %% mock up a put call and share put_post().
    postcondition(S, {call,x,put,[_Mbox, _EP, Str, _MT, Key, <<"remove_post()">>]}, Ret);
postcondition(_S, {call,_,clear,[_Mbox, _EP, _Str, _MT]}, ["OK"]) ->
    true;
postcondition(#state{d=D}, {call,_,keySet,[_Mbox, _EP, _Str, _MT]}, Ret) ->
    case Ret of
        ["OK", X] ->
            X2 = string:strip(string:strip(X, left, $[), right, $]),
            Ks = string:tokens(X2, ", "),
            lists:sort(Ks) == lists:sort([K || {K,_V} <- orddict:to_list(D)])
    end;
postcondition(#state{d=D}, {call,_,values,[_Mbox, _EP, _Str, _MT]}, Ret) ->
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
postcondition(#state{d=D}, {call,_,entrySet,[_Mbox, _EP, _Str, _MT]}, Ret) ->
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

next_state(S, _V, {call,_,reset,[_Mbox, _EP]}) ->
    S#state{reset_p=true};
next_state(S=#state{d=D}, _V, {call,_,put,[_Mbox, _EP, _Str, _MT, Key, Val]}) ->
    S#state{d=orddict:store(Key, Val, D)};
next_state(S=#state{d=D}, _V, {call,_,remove,[_Mbox, _EP, _Str, _MT, Key]}) ->
    S#state{d=orddict:erase(Key, D)};
next_state(S, _V, {call,_,clear,[_Mbox, _EP, _Str, _MT]}) ->
    S#state{d=orddict:new()};
next_state(S, _V, _NoSideEffectCall) ->
    S.

%%%%

reset(Mbox, Endpoint) ->
    %% io:format(user, "R", []),
    rpc(Mbox, reset, Endpoint).

reboot(Mbox, Endpoint) ->
    io:format(user, "r", []),
    rpc(Mbox, reboot, Endpoint).

put(Mbox, Endpoint, Stream, MapType, Key, Val) ->
    rpc(Mbox, Endpoint, Stream, MapType, ["put", Key ++ "," ++ Val]).

get(Mbox, Endpoint, Stream, MapType, Key) ->
    rpc(Mbox, Endpoint, Stream, MapType, ["get", Key]).

size(Mbox, Endpoint, Stream, MapType) ->
    rpc(Mbox, Endpoint, Stream, MapType, ["size"]).

isEmpty(Mbox, Endpoint, Stream, MapType) ->
    rpc(Mbox, Endpoint, Stream, MapType, ["isEmpty"]).

containsKey(Mbox, Endpoint, Stream, MapType, Key) ->
    rpc(Mbox, Endpoint, Stream, MapType, ["containsKey", Key]).

containsValue(Mbox, Endpoint, Stream, MapType, Value) ->
    rpc(Mbox, Endpoint, Stream, MapType, ["containsValue", Value]).

remove(Mbox, Endpoint, Stream, MapType, Key) ->
    rpc(Mbox, Endpoint, Stream, MapType, ["remove", Key]).

%% %% putAll() can't be tested because our ASCII protocol can't represent
%% %% the needed map.

clear(Mbox, Endpoint, Stream, MapType) ->
    rpc(Mbox, Endpoint, Stream, MapType, ["clear"]).

keySet(Mbox, Endpoint, Stream, MapType) ->
    rpc(Mbox, Endpoint, Stream, MapType, ["keySet"]).

values(Mbox, Endpoint, Stream, MapType) ->
    rpc(Mbox, Endpoint, Stream, MapType, ["values"]).

entrySet(Mbox, Endpoint, Stream, MapType) ->
    rpc(Mbox, Endpoint, Stream, MapType, ["entrySet"]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

prop() ->
    prop(smrmap, 1).

prop(MapType, MoreCmds) ->
    prop(MapType, MoreCmds, qc_java:local_mboxes(), qc_java:local_endpoint()).

prop(MapType, MoreCmds, Mboxes, Endpoint)
  when MapType == smrmap; MapType == fgmap ->
    %% Hmmmm, more_commands() doesn't appear to work correctly with Proper.
    ?FORALL(Cmds, more_commands(MoreCmds,
                                commands(?MODULE,
                                         initial_state(MapType,
                                                       Mboxes, Endpoint))),
            begin
                {H, S_or_Hs, Res} = run_commands(?MODULE, Cmds),
                aggregate(command_names(Cmds),
                measure(
                  cmds_length,
                  ?COMMANDS_LENGTH(Cmds),
                ?PRETTY_FAIL(
                  ?MODULE, Cmds, H,S_or_Hs,Res,
                  begin
                      Res == ok
                  end
                )))
            end).

prop_parallel() ->
    prop_parallel(smrmap, 1).

prop_parallel(MapType, MoreCmds) ->
    prop_parallel(MapType, MoreCmds, qc_java:local_mboxes(), qc_java:local_endpoint()).

prop_parallel(MapType, MoreCmds, Mboxes, Endpoint) ->
    AlwaysNum = 20,
    io:format(user, "NOTE: parallel cmds are executed ~w times to try to detect non-determinism\n", [AlwaysNum]),
    ?FORALL(Cmds, more_commands(MoreCmds,
                                parallel_commands(?MODULE,
                                         initial_state(MapType,
                                                       Mboxes, Endpoint))),
            ?WRAP_ALWAYS(AlwaysNum,
            begin
                {H, S_or_Hs, Res} = run_parallel_commands(?MODULE, Cmds),
                aggregate(command_names(Cmds),
                measure(
                  cmds_length,
                  ?COMMANDS_LENGTH(Cmds),
                ?PRETTY_FAIL(
                  ?MODULE, Cmds, H,S_or_Hs,Res,
                  begin
                      Res == ok
                  end
                )))
            end)).

seq_to_par_cmds(L) ->
    [Cmd || Cmd <- L,
            element(1, Cmd) /= init,
            element(3, element(3, Cmd)) /= reset].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

rpc(Mbox, reset, Endpoint) ->
    AllArgs = ["corfu_smrobject", "reset", Endpoint],
    qc_java:rpc_call(Mbox, AllArgs, ?TIMEOUT);
rpc(Mbox, reboot, Endpoint) ->
    AllArgs = ["corfu_smrobject", "reboot", Endpoint],
    qc_java:rpc_call(Mbox, AllArgs, ?TIMEOUT).

rpc({_RegName, _NodeName} = Mbox, Endpoint, Stream, MapType, Args) ->
    Class = if MapType == smrmap -> "org.corfudb.runtime.collections.SMRMap";
               MapType == fgmap  -> "org.corfudb.runtime.collections.FGMap"
            end,
    AllArgs = ["corfu_smrobject", "-c", Endpoint,
               %% -p = --quickcheck-ap-prefix
               "-p", lists:flatten(io_lib:format("~w", [Mbox])),
               "-s", integer_to_list(Stream),
               Class]
              ++ Args,
    qc_java:rpc_call(Mbox, AllArgs, ?TIMEOUT).
