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
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% APIs
-export([start_link/0]).

%% helpers
-export([bucket_id/1]).

-export([create/3, update/3, info/2, remove/2]).

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
    servers := #{limiter_type() => pid()},
    buckets := buckets(),
    tref := reference() | undefined
}.

-type buckets() :: #{tenant_id() => #{limiter_type() => bucket_info()}}.

-type bucket_info() :: #{
    server := pid(),
    rate := pos_integer(),
    allocated_rate := pos_integer(),
    latest_cluster_rate := float(),
    latest_node_rate := float(),
    latest_node_obtained := #{node() => non_neg_integer()}
}.

-define(DEFAULT_TIMEOUT, 5000).
-define(DEFAULT_SERVER_PERIOD, 1000).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec create(pid(), tenant_id(), limiter_config()) ->
    ok
    | {error, term()}.
create(Pid, TenantId, Cfg) ->
    call(Pid, {update, TenantId, convert_key(Cfg)}).

-spec update(pid(), tenant_id(), limiter_config()) -> ok | {error, term()}.
update(Pid, TenantId, Cfg) ->
    call(Pid, {update, TenantId, convert_key(Cfg)}).

-spec remove(pid(), tenant_id()) -> ok.
remove(Pid, TenantId) ->
    call(Pid, {remove, TenantId}).

-spec info(pid(), tenant_id()) -> [bucket_info()].
info(Pid, TenantId) ->
    call(Pid, {info, TenantId}).

call(Pid, Msg) ->
    gen_server:call(Pid, Msg).

convert_key(Cfg) ->
    maps:fold(fun(Key, Val, Acc) -> Acc#{type(Key) => Val} end, #{}, Cfg).

type(max_messages_in) -> message_in;
type(max_bytes_in) -> bytes_in.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

-spec init(list()) -> {ok, state()}.
init([]) ->
    process_flag(trap_exit, true),
    State = #{
        servers => #{},
        %% FIXME: load fisrt
        buckets => #{},
        tref => undefined
    },
    {ok, ensure_rebalance_timer(State)}.

handle_call({info, TenantId}, _From, State = #{buckets := Buckets}) ->
    Reply =
        case maps:get(TenantId, Buckets, undefined) of
            undefined ->
                undefined;
            BucketsInfo ->
                maps:fold(
                    fun(
                        Type,
                        #{
                            rate := Rate,
                            allocated_rate := ARate,
                            latest_cluster_rate := ClusterRate,
                            latest_node_rate := NodeRate
                        },
                        Acc
                    ) ->
                        Info = #{
                            type => Type,
                            rate => Rate,
                            allocated_rate => ARate,
                            latest_cluster_rate => ClusterRate,
                            latest_node_rate => NodeRate
                        },
                        [Info | Acc]
                    end,
                    [],
                    BucketsInfo
                )
        end,
    {reply, Reply, State};
handle_call(
    {update, TenantId, Cfg},
    _From,
    State = #{servers := Servers, buckets := Buckets}
) ->
    {Servers1, Buckets1} = do_update(maps:to_list(Cfg), TenantId, Servers, Buckets),
    {reply, ok, State#{servers := Servers1, buckets := Buckets1}};
handle_call(
    {remove, TenantId},
    _From,
    State = #{buckets := Buckets}
) ->
    Buckets1 =
        case maps:take(TenantId, Buckets) of
            error ->
                Buckets;
            {BucketsInfo, B} ->
                do_del_bucket(TenantId, BucketsInfo),
                B
        end,
    {reply, ok, State#{buckets := Buckets1}};
handle_call(_Request, _From, State) ->
    {reply, {error, unexpected_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(
    {timeout, _Ref, rebalance},
    State = #{servers := Servers, buckets := Buckets}
) ->
    %% #{tenant_id() => #{limiter_type() => float()}}
    Usage0 = collect_obtained(Servers),
    Usage1 = filter_out_unchanged_tenants(Usage0, Buckets),

    ok = emqx_tenancy_limiter_storage:insert(Usage1),
    Buckets1 = rebalance_changed_tenants(Usage1, Buckets),
    ok = batch_update_buckets_rate(Buckets1, Servers),

    Buckets2 = maps:merge(Buckets, Buckets1),
    ?tp(debug, rebalanced, #{count => maps:size(Usage1)}),
    {noreply, ensure_rebalance_timer(State#{buckets := Buckets2, tref := undefined})};
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

do_update([], _TenantId, Servers, Buckets) ->
    {Servers, Buckets};
do_update([{Type, Rate} | More], TenantId, Servers, Buckets) ->
    Server =
        case maps:get(Type, Servers, undefined) of
            undefined ->
                start_limiter_server(Type);
            Pid ->
                Pid
        end,
    ok = ensure_bucket_rate(Server, TenantId, Rate),

    Info = emqx_map_lib:deep_get([TenantId, Type], Buckets, #{}),
    Info1 = Info#{
        server => Server,
        rate => Rate,
        allocated_rate => Rate,
        latest_cluster_rate => 0,
        latest_node_rate => 0,
        latest_node_obtained => #{}
    },
    Buckets1 = emqx_map_lib:deep_put([TenantId, Type], Buckets, Info1),

    do_update(More, TenantId, Servers#{Type => Server}, Buckets1).

start_limiter_server(Type) ->
    LimiterCfg = #{
        rate => infinity,
        burst => 0,
        period => ?DEFAULT_SERVER_PERIOD
    },
    {ok, Pid} = emqx_limiter_server:start_link(
        noname,
        Type,
        LimiterCfg
    ),
    Pid.

ensure_bucket_rate(Pid, TenantId, Rate) ->
    ok = emqx_limiter_server:add_bucket(
        Pid,
        bucket_id(TenantId),
        bucket_cfg(Rate)
    ).

do_del_bucket(TenantId, BucketsInfo) ->
    %% FIXME: delete old stats
    maps:foreach(
        fun(_Type, _BucketInfo = #{server := Pid}) ->
            emqx_limiter_server:del_bucket(Pid, TenantId)
        end,
        BucketsInfo
    ).

batch_update_buckets_rate(Buckets, Servers) ->
    Rates =
        maps:fold(
            fun(TenantId, BucketsInfo, Acc0) ->
                maps:fold(
                    fun(Type, _BucketInfo = #{allocated_rate := Rate}, Acc1) ->
                        M = maps:get(Type, Acc1, #{}),
                        Acc1#{Type => M#{TenantId => inner_rate(Rate)}}
                    end,
                    Acc0,
                    BucketsInfo
                )
            end,
            #{},
            Buckets
        ),

    maps:foreach(
        fun(Type, Rates1) ->
            Pid = maps:get(Type, Servers),
            ok = emqx_limiter_server:update_buckets_rate(Pid, Rates1)
        end,
        Rates
    ).

%%--------------------------------------------------------------------
%% rebalance

ensure_rebalance_timer(State = #{tref := undefined}) ->
    Ref = emqx_misc:start_timer(?DEFAULT_TIMEOUT, rebalance),
    State#{tref := Ref}.

-type usage() :: #{tenant_id() => #{limiter_type() => float()}}.

-spec collect_obtained([pid()]) -> usage().
collect_obtained(Servers) ->
    maps:fold(
        fun(Type, Pid, Acc0) ->
            #{buckets := Buckets} = emqx_limiter_server:info(Pid),
            %% bucket name is TenantId
            maps:fold(
                fun(TenantId, #{obtained := Obtained}, Acc1) ->
                    AllTypes = maps:get(TenantId, Acc1, #{}),
                    Acc1#{TenantId => AllTypes#{Type => erlang:floor(Obtained)}}
                end,
                Acc0,
                Buckets
            )
        end,
        #{},
        Servers
    ).

-spec filter_out_unchanged_tenants(usage(), buckets()) -> usage().
filter_out_unchanged_tenants(Usage, Buckets) ->
    maps:filter(
        fun(TenantId, AllTypes) ->
            case maps:get(TenantId, Buckets, undefined) of
                undefined ->
                    true;
                BucketsInfo ->
                    is_new_tokens_obtained(BucketsInfo, maps:iterator(AllTypes))
            end
        end,
        Usage
    ).

is_new_tokens_obtained(BucketsInfo, Iter) ->
    case maps:next(Iter) of
        none ->
            false;
        {Type, Obtained, Iter1} ->
            Last = emqx_map_lib:deep_get([Type, latest_node_obtained, node()], BucketsInfo, 0),
            Obtained > Last orelse is_new_tokens_obtained(BucketsInfo, Iter1)
    end.

-spec rebalance_changed_tenants(usage(), buckets()) -> buckets().
rebalance_changed_tenants(Usage, Buckets) ->
    maps:map(
        fun(TenantId, TypedUsage) ->
            maps:map(
                fun(Type, Obtained) ->
                    BucketInfo = emqx_map_lib:deep_get([TenantId, Type], Buckets),
                    do_rebalance_tenant(TenantId, Type, Obtained, BucketInfo)
                end,
                TypedUsage
            )
        end,
        Usage
    ).

do_rebalance_tenant(
    TenantId,
    Type,
    _Obtained,
    #{
        rate := ClusterRate,
        allocated_rate := AllocatedRate
    } = BucketInfo
) ->
    LastNodesObtained = maps:get(latest_node_obtained, BucketInfo, #{}),
    LatestNodesObtained = all_nodes_obtained(TenantId, Type),

    Diff = obtained_tokens_per_node(LatestNodesObtained, LastNodesObtained),
    Rate = maps:map(fun(_Node, Deta) -> Deta * 1000 / ?DEFAULT_TIMEOUT end, Diff),

    LatestClusterRate = lists:sum(maps:values(Rate)),
    LatestNodeRate = maps:get(node(), Rate),

    AllowedNodeRate = allowed_node_rate(TenantId, ClusterRate),

    NAllocatedRate = calculate_new_allocated_rate(
        ClusterRate,
        AllowedNodeRate,
        AllocatedRate,
        LatestClusterRate,
        LatestNodeRate
    ),
    BucketInfo#{
        allocated_rate => NAllocatedRate,
        latest_cluster_rate => LatestClusterRate,
        latest_node_rate => LatestNodeRate,
        latest_node_obtained => LatestNodesObtained
    }.

all_nodes_obtained(TenantId, Type) ->
    lists:foldl(
        fun(Node, Acc) ->
            case emqx_tenancy_limiter_storage:lookup(Node, TenantId, Type) of
                undefined ->
                    Acc;
                Obtained ->
                    Acc#{Node => Obtained}
            end
        end,
        #{},
        [node() | nodes()]
    ).

allowed_node_rate(TenantId, ClusterRate) ->
    SessInNode = emqx_tenancy_quota:number_of_sessions(TenantId, node()),
    SessInCluster = emqx_tenancy_quota:number_of_sessions(TenantId),
    case {SessInNode, SessInCluster} of
        %% Set the initial value to tenant's rate
        {0, _} -> ClusterRate;
        %%
        {_, 0} -> ClusterRate;
        _ -> (SessInNode / SessInCluster) * ClusterRate
    end.

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

-spec bucket_id(tenant_id()) -> bucket_id().
bucket_id(TenantId) when is_binary(TenantId) ->
    TenantId.

bucket_cfg(Rate) ->
    #{
        rate => inner_rate(Rate),
        initial => 0,
        capacity => Rate
    }.

inner_rate(Rate) ->
    Rate / (1000 / ?DEFAULT_SERVER_PERIOD).
