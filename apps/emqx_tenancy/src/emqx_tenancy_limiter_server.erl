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
    config := limiter_config(),
    servers := #{limiter_type() => pid()},
    tref := reference() | undefined
}.

-define(STATS_TAB, emqx_tenancy_limiter_stats).

-record(stats, {
    key :: {node(), tenant_id()},
    type :: limiter_type(),
    obtained :: pos_integer()
}).

-define(DEFAULT_TIMEOUT, 5000).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_link(tenant_id(), limiter_config()) -> {ok, pid()} | ignore | {error, term()}.
start_link(TenantId, Cfg) ->
    gen_server:start_link(?MODULE, [TenantId, Cfg], []).

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
    ok = ensure_stats_table_created(),
    Servers = start_limiter_servers(Cfg),
    ok = init_buckets(TenantId, Cfg, Servers),
    State = #{
        tenant_id => TenantId,
        servers => Servers,
        config => Cfg,
        tref => undefined
    },
    {ok, ensure_calcu_timer(State)}.

ensure_stats_table_created() ->
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

handle_call(info, _From, State) ->
    %% TODO:
    {reply, [], State};
handle_call({update, _Cfg}, _From, State) ->
    %% TODO: check cfg? hot-upgrade
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(rebalance, State) ->
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

start_limiter_servers(Cfg) ->
    maps:map(
        fun(Key, Rate) ->
            Type = type(Key),
            LimiterCfg = #{rate => Rate / 10, burst => 0},
            {ok, Pid} = emqx_limiter_server:start_link(
                noname,
                Type,
                LimiterCfg
            ),
            Pid
        end,
        Cfg
    ).

%% @doc After a Bucket is added, it is automatically managed by
%% `emqx_limiter_manager`. In this way, it can be linked in
%% `emqx_tenancy_limiter:on_upgrade_limiters/2`
init_buckets(TenantId, Cfg, Servers) ->
    maps:foreach(
        fun(Key, Pid) ->
            Type = type(Key),
            Rate = maps:get(Key, Cfg),
            %% assert
            ok = emqx_limiter_server:add_bucket(
                Pid,
                bucket_id(TenantId, Type),
                bucket_cfg(Rate)
            )
        end,
        Servers
    ).

ensure_calcu_timer(State = #{tref := undefined}) ->
    Ref = emqx_misc:start_timer(?DEFAULT_TIMEOUT, rebalance),
    State#{tref := Ref}.

%% TODO: more types max_sub_in?
type(max_conn_rate) -> connection;
type(max_messages_in) -> message_in;
type(max_bytes_in) -> bytes_in.

-spec bucket_id(tenant_id(), limiter_type()) -> bucket_id().
bucket_id(TenantId, Type) ->
    <<TenantId/binary, "-", (atom_to_binary(Type))/binary>>.

bucket_cfg(Rate) ->
    InnerRate = Rate / 10,
    #{
        rate => InnerRate,
        initial => 0,
        capacity => Rate
    }.
