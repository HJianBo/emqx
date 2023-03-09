%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_tenancy_push).

-include("emqx_tenancy.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-behaviour(gen_server).

-export([start_link/0]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(LIMIT, 1000).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    ok = mria:wait_for_tables([?TENANCY]),
    sample_timer(),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({sample, Time}, State) ->
    sample(Time),
    sample_timer(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -------------------------------------------------------------------------------------------------

sample_timer() ->
    {NextTime, Remaining} = next_interval(),
    erlang:send_after(Remaining, self(), {sample, NextTime}).

%% Per interval seconds.
%% As an example:
%%  Interval = 10000
%%  The monitor will start working at full seconds, as like 00:00:00, 00:00:10, 00:00:20 ...
%% Ensure that the monitor data of all nodes in the cluster are aligned in time
next_interval() ->
    Interval = emqx_conf:get([tenant, sample_interval]),
    Now = erlang:system_time(millisecond),
    NextTime = ((Now div Interval) + 1) * Interval,
    Remaining = NextTime - Now,
    {NextTime div 1000, Remaining}.

sample(Time) ->
    Spec = ets:fun2ms(fun(#tenant{enabled = true, id = Id}) -> Id end),
    case ets:select(?TENANCY, Spec, ?LIMIT) of
        '$end_of_table' -> ok;
        {Tenants, Continuation} -> sample(Time, Tenants, Continuation)
    end.

sample(Time, Tenants, Continuation) ->
    Payload =
        emqx_json:encode(#{
            <<"timestamp">> => Time,
            <<"node">> => node(),
            <<"data">> => lists:map(fun emqx_tenancy_stats:sample/1, Tenants)
        }),
    Message = emqx_message:make(
        ?MODULE,
        0,
        <<"$SYS/$TENANT/metrics">>,
        Payload,
        #{sys => true},
        #{properties => #{'Content-Type' => <<"application/json">>}}
    ),
    _ = emqx_broker:safe_publish(Message),
    case ets:select(Continuation) of
        '$end_of_table' -> ok;
        {NewTenants, NewContinuation} -> sample(Time, NewTenants, NewContinuation)
    end.
