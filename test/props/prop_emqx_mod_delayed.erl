%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(prop_emqx_mod_delayed).

-include("emqx.hrl").
-include_lib("proper/include/proper.hrl").

-import(emqx_ct_proper_types,
        [ message/0
        , normal_topic/0
        ]).

%% Model callbacks
-export([ initial_state/0
        , command/1
        , precondition/2
        , postcondition/3
        , next_state/3
        ]).

-define(ALL(Vars, Types, Exprs),
        ?SETUP(fun() ->
            State = do_setup(),
            fun() -> do_teardown(State) end
         end, ?FORALL(Vars, Types, Exprs))).

-define(MAX_DELAYED, 5).

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_delayed() ->
    %dbg:tracer(),dbg:p(all,call),
    %dbg:tp(emqx_mod_delayed, on_message_publish,1,x),
    ?ALL(Cmds, commands(?MODULE),
        begin
            _ = emqx_mod_delayed:load([]),
            {History, State, Result} = run_commands(?MODULE, Cmds),

            %io:format("Delayed messages: ~p~n", [[Key ||{_, Key, _} <- ets:tab2list(emqx_mod_delayed)]]),
            Result1 = wait_for_delayed_message(State, Result),

            _ = emqx_mod_delayed:unload([]),
            ?WHENFAIL(io:format("\nState: ~p\nResult: ~p\n",
                                [State, Result1]),
                      aggregate(command_names(Cmds), Result1 =:= ok))
        end).

prop_others() ->
    "EMQ X Delayed Publish Module" =:= emqx_mod_delayed:description().

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

wait_for_delayed_message(_State, Result) when Result /= ok ->
    Result;
wait_for_delayed_message(#{msgs := []}, Result) ->
    Result;
wait_for_delayed_message(#{msgs := Delayed}, _) ->
    MaxT0 = lists:foldl(fun({Intv, _Msg}, T0) when Intv > T0 -> Intv;
                           ({_Intv, _Msg}, T0) -> T0
                        end, 0, Delayed),
    MaxT = (MaxT0 + 1) * 1000,
    MaxN = length(Delayed),
    RecvN = fun ReceiveFun([]) ->
                    ok;
                ReceiveFun(Expected) ->
                    receive
                        {deliver, _Topic, Msg = #message{topic = InTopic,
                                                         payload = InPayload}} ->
                            io:format("- recevied: ~p\n", [format(Msg)]),
                            ACKed  = [E || E = {_, #message{topic = Topic,
                                                               payload = Payload}}
                                            <- Expected, Topic =:= InTopic, Payload =:= InPayload],
                            ReceiveFun(Expected -- ACKed)
                    after MaxT ->
                         io:format("Remained messages: ~p\n", [Expected]),
                         {error, not_enough_delayed_messages}
                    end
            end,
    io:format("Wait delayed ~p messages: [\n", [MaxN]),
    [io:format("\t~s\n", [format(Msg)]) || {_, Msg} <- Delayed],
    io:format("]\n"),
    RecvN(Delayed).

format(#message{topic = Topic, payload = Payload}) ->
    lists:flatten(io_lib:format("topic=~s, payload=~0p", [Topic, Payload])).

do_setup() ->
    Self = self(),
    ekka:start(),
    application:ensure_all_started(gproc),
    meck:new(emqx, [passthrough, no_history]),
    meck:expect(emqx, hook, fun (_, _) -> ok end),
    meck:expect(emqx, unhook, fun (_, _) -> ok end),
    meck:expect(emqx, publish, fun (Msg) -> Self ! {deliver, Msg#message.topic, Msg} end),
    {ok, _} = emqx_pool_sup:start_link(),
    {ok, _} = emqx_mod_sup:start_link(),
    {ok, _} = emqx_metrics:start_link().

do_teardown(_) ->
    emqx_metrics:stop(),
    meck:unload(emqx),
    ekka:stop(),
    application:stop(gproc),
    mnesia:delete_table(emqx_mod_delayed),
    ok.

parse_delayed_topic(<<"$delayed/", Topic0/binary>>) ->
    [Intv0, Topic] = binary:split(Topic0, <<"/">>),
    case catch binary_to_integer(Intv0) of
        Intv when is_integer(Intv) -> {ok, Intv, Topic};
        Reason -> {error, Reason}
    end;
parse_delayed_topic(_) ->
    {error, not_a_delayed_topic}.

%%--------------------------------------------------------------------
%% Model
%%--------------------------------------------------------------------

initial_state() ->
    #{msgs => []}.

command(_State) ->
    M = emqx_mod_delayed,
    frequency([{99, {call, M, on_message_publish, [maybe_a_delayed_message()]}},
               %------------ unexpected message ----------------------%
               {1, {call, M, handle_call, [req, self(), state]}},
               {1, {call, M, handle_cast, [msg, state]}},
               {1, {call, M, handle_info, [msg, state]}}
              ]).

precondition(_State, {call, _M, _F, _Args}) ->
    true.

postcondition(_State, {call, emqx_mod_delayed, on_message_publish, [Msg]}, Res) ->
    case parse_delayed_topic(emqx_message:topic(Msg)) of
        {ok, _Intv, NTopic} ->
            Headers = Msg#message.headers,
            Res =:= {stop, Msg#message{
                             topic = NTopic,
                             headers = Headers#{allow_publish => false}}};
        _ ->
            Res =:= {ok, Msg}
    end;
postcondition(_State, {call, _M, _F, _Args}, _Res) ->
    true.

next_state(State = #{msgs := Delayed}, _Res, {call, _M, on_message_publish, [Msg]}) ->
    case parse_delayed_topic(emqx_message:topic(Msg)) of
        {ok, Intv, NTopic} ->
            State#{msgs := lists:sort([{Intv, Msg#message{topic = NTopic}} | Delayed])};
        _ ->
            State
    end;
next_state(State, _Res, {call, _M, _F, _Args}) ->
    State.

%%--------------------------------------------------------------------
%% Generators
%%--------------------------------------------------------------------

maybe_a_delayed_message() ->
    ?LET({Msg, Topic, Intv}, {message(), normal_topic(), range(1, ?MAX_DELAYED)},
        begin
            NTopic =
                case rand:uniform(1) =:= 1 of
                    true ->
                        <<"$delayed/", (integer_to_binary(Intv))/binary, "/", Topic/binary>>;
                    _ -> Topic 
                end,
            Msg#message{topic = NTopic}
        end).

