%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_tenancy_limiter_server).

-behaviour(gen_server).

-include("emqx_tenancy.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% APIs
-export([start_link/2]).

-export([update/2, info/1]).

%% helpers
-export([bucket_id/2]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-type state() :: #{
    tenant_id := tenant_id(),
    limiters := [limiter()],
    last_stats := #{limiter_type() => #{node() => pos_integer()}},
    tref := reference() | undefined
}.

-define(STATS_TAB, emqx_tenancy_limiter_stats).

-record(stats, {
    key :: {node(), tenant_id()},
    type :: limiter_type(),
    %% defines at emqx_limiter_server:bucket()
    obtained :: float()
}).

-type limiter() :: #{
    rate := pos_integer(),
    allocated_rate := pos_integer(),
    server := pid(),
    latest_cluster_rate := float(),
    latest_node_rate := float()
}.

-define(DEFAULT_TIMEOUT, 5000).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_link(tenant_id(), limiter_config()) -> {ok, pid()} | ignore | {error, term()}.
start_link(TenantId, Cfg) ->
    gen_server:start_link(?MODULE, [TenantId, convert_key(Cfg)], []).

convert_key(Cfg) ->
    maps:fold(fun(Key, Val, Acc) -> Acc#{type(Key) => Val} end, #{}, Cfg).

%% TODO: more types max_sub_in?
type(max_conn_rate) -> connection;
type(max_messages_in) -> message_in;
type(max_bytes_in) -> bytes_in.

-spec update(pid(), limiter_config()) -> ok | {error, term()}.
update(Pid, Cfg) ->
    call(Pid, {update, Cfg}).

-spec info(pid()) -> [limiter_info()].
info(Pid) ->
    call(Pid, info).

call(Pid, Msg) ->
    gen_server:call(Pid, Msg).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

-spec init(list()) -> {ok, state()}.
init([TenantId, Cfg]) ->
    process_flag(trap_exit, true),
    ok = ensure_table_created(),
    Limiters = start_limiter_servers(TenantId, Cfg),
    State = #{
        tenant_id => TenantId,
        limiters => Limiters,
        last_stats => #{},
        tref => undefined
    },
    {ok, ensure_rebalance_timer(State)}.

ensure_table_created() ->
    ok = mria:create_table(?STATS_TAB, [
        {type, set},
        {rlog_shard, ?TENANCY_SHARD},
        {storage, ram_copies},
        {record_name, stats},
        {attributes, record_info(fields, stats)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]).

handle_call(info, _From, State = #{limiters := Limiters}) ->
    Infos = maps:fold(
        fun(
            Type,
            #{
                rate := Rate,
                allocated_rate := ARate,
                latest_cluster_rate := ClusterRate,
                latest_node_rate := NodeRate,
                server := Pid
            },
            Acc
        ) ->
            Info = #{
                type => Type,
                rate => Rate,
                allocated_rate => ARate,
                latest_cluster_rate => ClusterRate,
                latest_node_rate => NodeRate,
                %% XXX: Other platforms not support big-integer ?
                obtained => lookup_obtained(Pid)
            },
            [Info | Acc]
        end,
        [],
        Limiters
    ),
    {reply, Infos, State};
handle_call({update, _Cfg}, _From, State) ->
    %% TODO: check cfg? hot-upgrade
    %% add-ons update
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unexpected_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, _Ref, rebalance}, State = #{tenant_id := TenantId, limiters := Limiters}) ->
    Usages = collect_obtained(Limiters),
    NewStats = fetch_current_stats(TenantId),
    NState = rebalance(Usages, NewStats, State),
    ok = report_usage(TenantId, Usages),
    {noreply, ensure_rebalance_timer(NState#{tref := undefined})};
%% TODO: handle process down
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

start_limiter_servers(TenantId, Cfg) ->
    maps:map(
        fun(Type, Rate) ->
            LimiterCfg = #{rate => inner_rate(Rate), burst => 0},
            {ok, Pid} = emqx_limiter_server:start_link(
                noname,
                Type,
                LimiterCfg
            ),
            ok = set_bucket_rate(TenantId, Type, Pid, Rate),
            #{
                rate => Rate,
                allocated_rate => Rate,
                server => Pid,
                latest_cluster_rate => 0,
                latest_node_rate => 0
            }
        end,
        Cfg
    ).

set_bucket_rate(TenantId, Type, Pid, Rate) ->
    ok = emqx_limiter_server:add_bucket(
        Pid,
        bucket_id(TenantId, Type),
        bucket_cfg(Rate)
    ).

%%--------------------------------------------------------------------
%% rebalance

ensure_rebalance_timer(State = #{tref := undefined}) ->
    Ref = emqx_misc:start_timer(?DEFAULT_TIMEOUT, rebalance),
    State#{tref := Ref}.

fetch_current_stats(TenantId) ->
    Ms = ets:fun2ms(
        fun(#stats{key = {Node, TId}, type = Type, obtained = Obtained}) when TId =:= TenantId ->
            {Node, Type, Obtained}
        end
    ),
    lists:foldl(
        fun({Node, Type, Obtained}, Acc) ->
            M = maps:get(Type, Acc, #{}),
            Acc#{Type => M#{Node => Obtained}}
        end,
        #{},
        ets:select(?STATS_TAB, Ms)
    ).

collect_obtained(Limiters) ->
    maps:map(fun(_, #{server := Pid}) -> lookup_obtained(Pid) end, Limiters).

lookup_obtained(Pid) ->
    #{buckets := Buckets} = emqx_limiter_server:info(Pid),
    %% assert: only one bucket in limiter server
    [#{obtained := Obtained}] = maps:values(Buckets),
    Obtained.

report_usage(TenantId, Usages) ->
    maps:foreach(
        fun(Type, Obtained) ->
            Stats = #stats{key = {node(), TenantId}, type = Type, obtained = Obtained},
            mria:dirty_write(?STATS_TAB, Stats)
        end,
        Usages
    ).

rebalance(Usages, NewStats, State) when is_map(Usages) ->
    rebalance(maps:to_list(Usages), NewStats, State);
rebalance([], _, State) ->
    State;
rebalance(
    [{Type, Obtained} | More],
    NewStats0,
    State = #{tenant_id := TenantId, limiters := Limiters, last_stats := LastStats0}
) ->
    #{
        rate := ClusterRate,
        allocated_rate := AllocatedRate,
        server := ServerPid
    } = Limiter = maps:get(Type, Limiters),

    AllowedNodeRate = allowed_node_rate(Type, ClusterRate, State),

    LastStats = maps:get(Type, LastStats0, #{}),
    NewStats = maps:put(node(), Obtained, maps:get(Type, NewStats0, #{})),

    Diff = obtained_tokens_per_node(NewStats, LastStats),
    Rate = maps:map(fun(_Node, Deta) -> Deta * 1000 / ?DEFAULT_TIMEOUT end, Diff),

    LatestClusterRate = lists:sum(maps:values(Rate)),
    LatestNodeRate = maps:get(node(), Rate),

    NAllocatedRate = calculate_new_allocated_rate(
        ClusterRate,
        AllowedNodeRate,
        AllocatedRate,
        LatestClusterRate,
        LatestNodeRate
    ),
    ok = set_bucket_rate(TenantId, Type, ServerPid, NAllocatedRate),
    NLimiter = Limiter#{
        allocated_rate => NAllocatedRate,
        latest_cluster_rate => LatestClusterRate,
        latest_node_rate => LatestNodeRate
    },
    rebalance(More, NewStats0, State#{
        limiters := Limiters#{Type => NLimiter},
        last_stats := LastStats0#{Type => NewStats}
    }).

%% Averaging by number of node connections
allowed_node_rate(_Type, ClusterRate, _) ->
    %% FIXME:
    ClusterRate / length(mria_mnesia:running_nodes()).

obtained_tokens_per_node(NewStats, LastStats) ->
    maps:fold(
        fun(Node, New, Acc) ->
            case maps:get(Node, Acc, 0) of
                0 ->
                    Acc#{Node => New};
                Old ->
                    case New - Old of
                        Diff when Diff < 0 ->
                            Acc#{Node => New};
                        Diff ->
                            Acc#{Node => Diff}
                    end
            end
        end,
        LastStats,
        NewStats
    ).

calculate_new_allocated_rate(
    ClusterRate,
    AllowedNodeRate,
    AllocatedRate,
    LatestClusterRate,
    LatestNodeRate
) ->
    case LatestClusterRate - ClusterRate of
        ExccedRate when ExccedRate > 0 ->
            %% Penalize myself if I exceed the expected rate
            case LatestNodeRate > AllowedNodeRate of
                true ->
                    AllowedNodeRate;
                false ->
                    AllocatedRate
            end;
        _ ->
            speed_up(AllocatedRate, ClusterRate)
    end.

speed_up(CurrentRate, ClusterRate) ->
    New = erlang:floor(CurrentRate * 1.5),
    case New < ClusterRate of
        true ->
            New;
        _ ->
            ClusterRate
    end.

%%--------------------------------------------------------------------
%% helpers

-spec bucket_id(tenant_id(), limiter_type()) -> bucket_id().
bucket_id(TenantId, Type) ->
    <<TenantId/binary, "-", (atom_to_binary(Type))/binary>>.

bucket_cfg(Rate) ->
    #{
        rate => inner_rate(Rate),
        initial => 0,
        capacity => Rate
    }.

%% rate per 100ms
inner_rate(Rate) ->
    Rate / 10.
