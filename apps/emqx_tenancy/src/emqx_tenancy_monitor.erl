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

-module(emqx_tenancy_monitor).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").
-include_lib("emqx/include/logger.hrl").

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

-export([
    current_rate/1,
    current_rate/2
]).

current_rate(Tenant) ->
    Fun =
        fun
            (Node, Cluster) when is_map(Cluster) ->
                case current_rate(Node, Tenant) of
                    {ok, CurrentRate} ->
                        merge_cluster_rate(CurrentRate, Cluster);
                    {badrpc, Reason} ->
                        {badrpc, {Node, Reason}}
                end;
            (_Node, Error) ->
                Error
        end,
    case lists:foldl(Fun, #{}, mria_mnesia:cluster_nodes(running)) of
        {badrpc, Reason} ->
            {badrpc, Reason};
        Rate ->
            {ok, Rate}
    end.

current_rate(all, Tenant) ->
    current_rate(Tenant);
current_rate(Node, Tenant) when Node == node() ->
    try
        {ok, sample(Tenant)}
    catch
        _E:R ->
            ?SLOG(warning, #{msg => "tenancy monitor error", reason => R, tenant => Tenant}),
            %% Rate map 0, ensure api will not crash.
            %% When joining cluster, tenancy monitor restart.
            Rate0 = [
                {Key, 0}
             || Key <- ?GAUGE_SAMPLER_LIST ++ maps:values(?DELTA_SAMPLER_RATE_MAP)
            ],
            {ok, maps:from_list(Rate0)}
    end;
current_rate(Node, Tenant) ->
    case emqx_tenancy_proto_v1:current_rate(Node, Tenant) of
        {badrpc, Reason} ->
            {badrpc, {Node, Reason}};
        {ok, Rate} ->
            {ok, Rate}
    end.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    sample_timer(),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({sample, Interval}, State) ->
    emqx_tenancy_stats:update_rate_stats(Interval),
    sample_timer(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

merge_cluster_rate(Node, Cluster) ->
    Fun =
        fun
            (topics, Value, NCluster) ->
                NCluster#{topics => Value};
            (Key, Value, NCluster) ->
                ClusterValue = maps:get(Key, NCluster, 0),
                NCluster#{Key => Value + ClusterValue}
        end,
    maps:fold(Fun, Cluster, Node).

sample_timer() ->
    {Interval, Remaining} = next_interval(),
    erlang:send_after(Remaining, self(), {sample, Interval}).

%% Per interval seconds.
%% As an example:
%%  Interval = 10
%%  The monitor will start working at full seconds, as like 00:00:00, 00:00:10, 00:00:20 ...
%% Ensure that the monitor data of all nodes in the cluster are aligned in time
next_interval() ->
    Second = emqx_conf:get([dashboard, sample_interval], ?DEFAULT_SAMPLE_INTERVAL),
    Interval = Second * 1000,
    Now = erlang:system_time(millisecond),
    NextTime = ((Now div Interval) + 1) * Interval,
    Remaining = NextTime - Now,
    {Second, Remaining}.

sample(Tenant) ->
    Sample = emqx_tenancy_stats:sample(Tenant),
    maps:with(
        [
            received_msg_rate,
            sent_msg_rate,
            dropped_msg_rate,
            subscriptions,
            subscriptions_shared,
            topics,
            connections,
            sessions,
            msg_retained
        ],
        Sample
    ).
