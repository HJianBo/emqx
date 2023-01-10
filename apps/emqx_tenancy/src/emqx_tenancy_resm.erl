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

%% @doc Tenant resources manager. It's responsible for
%% - Monitor tenant connections
%% - Clear connections once tenant deleted/disabled
-module(emqx_tenancy_resm).

-behaviour(gen_server).

-include("emqx_tenancy.hrl").

-define(BATCH_SIZE, 100000).

%% APIs
-export([start_link/0]).
-export([monitor_session_proc/3]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(CONNS_TAB, emqx_tenancy_conns).

-type state() :: #{
    pmon := emqx_pmon:pmon()
}.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Connections Monitor

-spec monitor_session_proc(pid(), emqx_types:tenant_id(), emqx_types:clientid()) -> ok.
monitor_session_proc(Pid, TenantId, ClientId) when TenantId =/= ?NO_TENANT ->
    ets:insert(?CONNS_TAB, {TenantId, Pid, ClientId}),
    cast({monitor_session_proc, Pid, TenantId, ClientId}).

cast(Msg) ->
    gen_server:cast(?MODULE, Msg).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

-spec init([]) -> {ok, state()}.
init([]) ->
    process_flag(trap_exit, true),
    TabOpts = [public, {write_concurrency, true}],
    ok = emqx_tables:new(?CONNS_TAB, [bag, {read_concurrency, true} | TabOpts]),
    %% subscribe emqx_tenancy table events
    {ok, _} = mnesia:subscribe({table, ?TENANCY, simple}),
    %% subscribe node running state
    ok = ekka:monitor(membership),
    {ok, #{pmon => emqx_pmon:new()}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({monitor_session_proc, Pid, TenantId, ClientId}, State = #{pmon := PMon}) ->
    PMon1 = emqx_pmon:monitor(Pid, {TenantId, ClientId}, PMon),
    {noreply, State#{pmon := PMon1}};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% there may be some schema events once the core node leaves.
handle_info({mnesia_table_event, {write, {schema, ?TENANCY, _Schmea}, _ActivityId}}, State) ->
    {noreply, State};
handle_info({mnesia_table_event, {delete, {schema, ?TENANCY}, _ActivityId}}, State) ->
    {noreply, State};
handle_info({mnesia_table_event, {write, Tenant, _ActivityId}}, State) ->
    %% Note: the Tenant's record name is emqx_tenacy not tenant
    case fix_record_name(Tenant) of
        #tenant{id = TenantId, enabled = false} ->
            kick_all_sessions(TenantId);
        _ ->
            ok
    end,
    {noreply, State};
handle_info({mnesia_table_event, {delete, {_Tab, TenantId}, _ActivityId}}, State) ->
    kick_all_sessions(TenantId),
    {noreply, State};
handle_info({'DOWN', _MRef, process, Pid, _Reason}, State = #{pmon := PMon}) ->
    ChanPids = [Pid | emqx_misc:drain_down(?BATCH_SIZE)],
    {Items, PMon1} = emqx_pmon:erase_all(ChanPids, PMon),
    ok = release_conns(Items),
    {noreply, State#{pmon := PMon1}};
handle_info({membership, {node, down, Node}}, State) ->
    case mria_membership:leader() of
        Leader when Leader == node() ->
            emqx_tenancy_quota:cleanup_node_down(Node);
        _ ->
            ok
    end,
    {noreply, State};
handle_info(_Info, State) ->
    %?SLOG(warning, #{msg => "unexpected_event", event => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

release_conns(Items) ->
    lists:foreach(
        fun({Pid, {TenantId, ClientId}}) ->
            _ = emqx_tenancy_quota:on_quota_sessions(
                release,
                #{tenant_id => TenantId},
                ok
            ),
            ets:delete_object(?CONNS_TAB, {TenantId, Pid, ClientId})
        end,
        Items
    ).

kick_all_sessions(TenantId) ->
    List = ets:lookup(?CONNS_TAB, TenantId),
    lists:foreach(
        fun({_, _Pid, ClientId}) ->
            emqx_cm:kick_session(ClientId)
        end,
        List
    ),
    ?SLOG(info, #{
        msg => "kick_all_sessions",
        tenant_id => TenantId,
        count => length(List)
    }).

fix_record_name(T) ->
    setelement(1, T, tenant).
