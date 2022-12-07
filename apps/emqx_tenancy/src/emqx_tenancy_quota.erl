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

-module(emqx_tenancy_quota).

-behaviour(gen_server).

%% APIs
-export([start_link/0]).
-export([create/1, update/1, remove/1, info/1]).

-define(USAGE_TAB, emqx_tenancy_usage).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% hooks callback
-export([
    on_quota_connections/3,
    on_quota_authn_users/3,
    on_quota_authz_users/3
    %on_quota_subs/4,
    %on_quota_retained/4,
    %on_quota_rules/4,
    %on_quota_resources/4
]).

-type state() :: #{
    pmon := emqx_pmon:pmon(),
    %% 0 means sync to mnesia immediately
    delayed_submit_ms := non_neg_integer(),
    %% How much delayed count is allowed
    delayed_submit_diff := integer(),
    %% local schema and buffer
    buffer := #{tenant_id() => usage()}
}.

%%--------------------------------------------------------------------
%% Mnesia

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec create(tenant_id(), quota_config()) -> ok | {error, term()}.
create(TenantId, Config) ->
    call({create, TenantId, Config}).

-spec update(tenant_id(), quota_config()) -> ok | {error, term()}.
update(TenantId, Config) ->
    call({create, TenantId, Config}).

-spec remove(tenant_id()) -> ok.
remove(TenantId) ->
    call({remove, TenantId}).

-type usage_counter() :: #{max := pos_integer(), used := non_neg_integer()}.
-type usage() :: #{
    connections := usage_counter(),
    authn_users := usage_counter(),
    authz_users := usage_counter()
    %subs := usage_counter(),
    %retained := usage_counter(),
    %rules := usage_counter(),
    %resources := usage_counter(),
    %shared_subs := usage_counter()
}.

-spec info(tenant_id()) -> usage().
info(TenantId) ->
    call({info, TenantId}).

call(Msg) ->
    gen_server:call(?MODULE, Msg).

cast(Msg) ->
    gen_server:cast(?MODULE, Msg).

%%--------------------------------------------------------------------
%% Hooks Callback

-record(usage, {
    id,
    connections,
    authn_users,
    authz_users,
    subs,
    retained,
    rules,
    resources,
    shared_subs
}).

-type quota_action() :: acquire | release.

-type permision() :: {stop, allow | deny} | ok.

-spec on_quota_resources(quota_action(), emqx_types:clientinfo(), term()) -> permision().
on_quota_connections(Action, _ClientInfo = #{tenant_id := TenantId}, _LastPermision) when
    Action =:= acquire; Action =:= release
->
    Action(TenantId, connections).

-spec on_quota_authn_users(quota_action(), tenant_id(), term(), term()) -> permision().
on_quota_authn_users(Action, TenantId, _UserInfo, _LastPermision) when
    Action =:= acquire; Action =:= release
->
    Action(TenantId, authn_users).

-spec on_quota_authz_users(quota_action(), tenant_id(), term(), term()) -> permision().
on_quota_authz_users(Action, TenantId, _Rule, _LastPermision) when
    Action =:= acquire; Action =:= release
->
    Action(TenantId, authz_users).

%% TODO: more hooks
%% ...

request(TennatId, Resource) ->
    case ets:lookup(?USAGE_TAB, TenantId, position(Resource)) of
        [#{max := Max, used := Used}] when Max > Used ->
            cast({acquire, TenantId, Resource, self()}),
            {stop, allow};
        _ ->
            {stop, deny}
    end.

release(TenantId, Resource) ->
    cast({release, TenantId, Resource}).

position(Resource) ->
    P = #{
        connections => #usage.connections,
        authn_users => #usage.authn_users,
        authz_users => #usage.authz_users,
        subs => #usage.subs,
        retained => #usage.retained,
        rules => #usage.rules,
        resources => #usage.resources,
        shared_subs => #usage.shared_subs
    },
    maps:get(Resource, P).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    ok = mria:create_table(?USAGE_TAB, [
        {type, set},
        {rlog_shard, ?TENANCY_SHARD},
        %% FIXME: ram_copies enough?
        %% but authn,authz_users, retained, etc. is disc_copies table
        {storage, disc_copies},
        {record_name, usage},
        {attributes, record_info(fields, usage)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    State = #{
        pmon => emqx_pmon:new(),
        delayed_submit_ms => 500,
        delayed_submit_diff => 10,
        buffer => load_tenants_quota(ets:tab2list(?TENANCY), #{})
    },
    {ok, State}.

load_tenants_quota([], Buffer) ->
    Buffer;
load_tenants_quota([#tenant{id = Id, quota = Config0} | More], Buffer) ->
    Config = with_quota_config(Config0),
    load_tenants_quota(More, Buffer#{Id => config_to_usage(Config)}).

handle_call(
    {create, TenantId, Config},
    _From,
    State = #{buffer := Buffer}
) ->
    Usage = config_to_usage(Config),
    case maps:get(TenantId, Buffer, undefined) of
        undefined ->
            ok = trans(fun() -> mnesia:write(?USAGE_TAB, Usage, write) end);
        BufferedUsage ->
            Usage1 = update_usage_record(Usage, BufferedUsage),
            ok = trans(fun() ->
                case mnesia:wread(?USAGE_TAB, TenantId) of
                    [] ->
                        mnesia:write(?USAGE_TAB, Usage1, write);
                    [TotalUsage0] ->
                        TotalUsage = update_usage_record(Usage1, TotalUsage0),
                        mnesia:write(?USAGE_TAB, TotalUsage, write)
                end
            end)
    end,
    {reply, ok, State#{buffer := Buffer#{TenantId => Usage}}};
handle_call(
    {remove, TenantId},
    _From,
    State
) ->
    {reply, ok, State};
handle_call({info, TenantId}, _From, State) ->
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({acquire, TenantId, Resource, From}, State) ->
    {noreply, State};
handle_cast({release, TenantId, Resource}, State) ->
    {noreply, State};
handle_info(submit, State) ->
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

%% @doc use New's max, and Old's + New's used
update_usage_record(New, Old) ->
    [_ | L1] = tuple_to_list(New),
    [_ | L2] = tuple_to_list(Old),
    L3 = lists:reverse(
        lists:foldl(
            fun(#{max := Max, used := Used0}, {N, Acc}) ->
                #{used := Used1} = lists:nth(N, L2),
                {N + 1, #{max => Max, used => Used0 + Used1}}
            end,
            {1, []},
            L1
        )
    ),
    list_to_tuple([usage | L3]).

%%--------------------------------------------------------------------
%% helpers

trans(Fun) ->
    case mria:transaction(?COMMON_SHARD, Fun) of
        {atomic, ok} -> ok;
        {atomic, Res} -> {ok, Res};
        {aborted, Error} -> {error, Error}
    end.

config_to_usage(
    TenantId,
    #{
        max_connections := MaxConns,
        max_authn_users := MaxAuthN,
        max_authz_users := MaxAuthZ,
        max_subscriptions := MaxSubs,
        max_retained_messages := MaxRetained,
        max_rules := MaxRules,
        max_resources := MaxRes,
        max_shared_subscriptions := MaxSharedSub
    }
) ->
    #usage{
        id = TenantId,
        connections = counter(MaxConns),
        authn_users = counter(MaxAuthN),
        authz_users = counter(MaxAuthZ),
        subscriptions = counter(MaxSubs),
        retained = counter(MaxRetained),
        rules = counter(MaxRules),
        resources = counter(MaxRes),
        shared_subs = counter(MaxSharedSub)
    }.

with_quota_config(Config) when is_map(Config) ->
    Keys = [
        <<"max_connection">>,
        <<"max_authn_users">>,
        <<"max_authz_users">>,
        <<"max_subscriptions">>,
        <<"max_retained_messages">>,
        <<"max_rules">>,
        <<"max_resources">>,
        <<"max_shared_subscriptions">>
    ],
    emqx_map_lib:safe_atom_key_map(maps:with(Keys, Config)).

counter(Max) ->
    #{max => Max, used => 0}.
