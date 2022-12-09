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

-include("emqx_tenancy.hrl").

-behaviour(gen_server).

%% APIs
-export([start_link/0]).
-export([load/0, unload/0]).

%% Management APIs
-export([create/2, update/2, remove/1, info/1]).

%% Node level APIs
-export([do_create/2, do_update/2, do_remove/1, do_info/1]).

-define(USAGE_TAB, emqx_tenancy_usage).

-define(BATCH_SIZE, 100000).

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
    on_quota_authn_users/4,
    on_quota_authz_users/4
    %% TODO: more hooks
    %on_quota_subs/4,
    %on_quota_retained/4,
    %on_quota_rules/4,
    %on_quota_resources/4
]).
-export([acquire/2, release/2]).

-type state() :: #{
    pmon := emqx_pmon:pmon(),
    %% 0 means sync to mnesia immediately
    delayed_submit_ms := non_neg_integer(),
    %% How much delayed count is allowed
    delayed_submit_diff := integer(),
    %% local schema and buffer
    buffer := #{tenant_id() => {usage(), LastSubmitTs :: pos_integer()}}
}.

-record(usage, {
    id :: tenant_id(),
    connections :: usage_counter(),
    authn_users :: usage_counter(),
    authz_users :: usage_counter(),
    subs :: usage_counter(),
    retained :: usage_counter(),
    rules :: usage_counter(),
    resources :: usage_counter(),
    shared_subs :: usage_counter()
}).

-type usage() :: #usage{}.

-type usage_info() :: #{
    connections := usage_counter(),
    authn_users := usage_counter(),
    authz_users := usage_counter(),
    subs := usage_counter(),
    retained := usage_counter(),
    rules := usage_counter(),
    resources := usage_counter(),
    shared_subs := usage_counter()
}.

-type usage_counter() :: #{max := pos_integer(), used := non_neg_integer()}.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% load/unload

%% @doc Load tenancy limiter
%% It's only works for new connections
-spec load() -> ok.
load() ->
    emqx_hooks:put('quota.connections', {?MODULE, on_quota_connections, []}, 0),
    emqx_hooks:put('quota.authn_users', {?MODULE, on_quota_authn_users, []}, 0),
    emqx_hooks:put('quota.authz_users', {?MODULE, on_quota_authz_users, []}, 0).

%% @doc Unload tenanct limiter
%% It's only works for new connections
-spec unload() -> ok.
unload() ->
    emqx_hooks:del('quota.connections', {?MODULE, on_quota_connections, []}),
    emqx_hooks:del('quota.authn_users', {?MODULE, on_quota_authn_users, []}),
    emqx_hooks:del('quota.authz_users', {?MODULE, on_quota_authz_users, []}).

%%--------------------------------------------------------------------
%% Management APIs (cluster-level)

-spec create(tenant_id(), quota_config()) -> ok.
create(TenantId, Config) ->
    multicall(?MODULE, do_create, [TenantId, Config]).

-spec update(tenant_id(), quota_config()) -> ok.
update(TenantId, Config) ->
    multicall(?MODULE, do_update, [TenantId, Config]).

-spec remove(tenant_id()) -> ok.
remove(TenantId) ->
    multicall(?MODULE, do_remove, [TenantId]).

-spec info(tenant_id()) -> {ok, #{node() => usage_info()}} | {error, term()}.
info(TenantId) ->
    Nodes = mria_mnesia:running_nodes(),
    case emqx_rpc:multicall(Nodes, ?MODULE, do_info, [TenantId]) of
        {Result, []} ->
            NodesResult = lists:zip(Nodes, Result),
            case return_ok_or_error(NodesResult) of
                ok ->
                    lists:foldl(
                        fun({Node, {ok, Info}}, Acc) ->
                            Acc#{Node => Info}
                        end,
                        #{},
                        NodesResult
                    );
                {error, Reason} ->
                    {error, Reason}
            end;
        {_, [Node | _]} ->
            {error, {Node, badrpc}}
    end.

multicall(M, F, A) ->
    Nodes = mria_mnesia:running_nodes(),
    case emqx_rpc:multicall(Nodes, M, F, A) of
        {Result, []} -> return_ok_or_error(lists:zip(Nodes, Result));
        {_, [Node | _]} -> {error, {Node, badrpc}}
    end.

return_ok_or_error(Result) ->
    Pred = fun
        ({_, {error, _}}) -> true;
        ({_, _}) -> false
    end,
    case lists:filter(Pred, Result) of
        [] -> ok;
        [{Node, {error, Reason}} | _] -> {error, {Node, Reason}}
    end.

%%--------------------------------------------------------------------
%% Management APIs (node-level)

-spec do_create(tenant_id(), quota_config()) -> ok.
do_create(TenantId, Config) ->
    call({create, TenantId, Config}).

-spec do_update(tenant_id(), quota_config()) -> ok.
do_update(TenantId, Config) ->
    call({create, TenantId, Config}).

-spec do_remove(tenant_id()) -> ok.
do_remove(TenantId) ->
    call({remove, TenantId}).

-spec do_info(tenant_id()) -> {ok, usage_info()} | {error, not_found}.
do_info(TenantId) ->
    case ets:lookup(?USAGE_TAB, TenantId) of
        [] ->
            {error, not_found};
        [Usage] ->
            {ok, usage_to_info(Usage)}
    end.

call(Msg) ->
    gen_server:call(?MODULE, Msg).

cast(Msg) ->
    gen_server:cast(?MODULE, Msg).

%%--------------------------------------------------------------------
%% Hooks Callback

-type quota_action() :: acquire | release.

-type permision() :: {stop, allow | deny} | ok.

-spec on_quota_connections(quota_action(), emqx_types:clientinfo(), term()) -> permision().
on_quota_connections(Action, _ClientInfo = #{tenant_id := TenantId}, _LastPermision) when
    Action =:= acquire; Action =:= release
->
    ?MODULE:Action(TenantId, connections).

-spec on_quota_authn_users(quota_action(), tenant_id(), term(), term()) -> permision().
on_quota_authn_users(Action, TenantId, _UserInfo, _LastPermision) when
    Action =:= acquire; Action =:= release
->
    ?MODULE:Action(TenantId, authn_users).

-spec on_quota_authz_users(quota_action(), tenant_id(), term(), term()) -> permision().
on_quota_authz_users(Action, TenantId, _Rule, _LastPermision) when
    Action =:= acquire; Action =:= release
->
    ?MODULE:Action(TenantId, authz_users).

acquire(TenantId, Resource) ->
    try ets:lookup_element(?USAGE_TAB, TenantId, position(Resource)) of
        #{max := Max, used := Used} when Max > Used ->
            cast({acquired, TenantId, Resource, self()}),
            {stop, allow};
        _ ->
            {stop, deny}
    catch
        error:badarg ->
            {stop, deny}
    end.

release(TenantId, Resource) ->
    cast({released, TenantId, Resource}).

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

-spec init([]) -> {ok, state()}.
init([]) ->
    process_flag(trap_exit, true),
    ok = mria:create_table(?USAGE_TAB, [
        {type, set},
        {rlog_shard, ?TENANCY_SHARD},
        %% FIXME: ram_copies enough?
        %% but authn,authz_users, retained, etc. is disc_copies table
        {storage, ram_copies},
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
        buffer => #{}
    },
    {ok, ensure_submit_timer(State)}.

handle_call(
    {create, TenantId, Config},
    _From,
    State = #{buffer := Buffer}
) ->
    Usage = config_to_usage(TenantId, Config),
    case maps:get(TenantId, Buffer, undefined) of
        undefined ->
            ok = trans(fun() -> mnesia:write(?USAGE_TAB, Usage, write) end);
        {BufferedUsage, _} ->
            Usage1 = update_usage_record(Usage, BufferedUsage),
            ok = trans(fun() ->
                case mnesia:read(?USAGE_TAB, TenantId, write) of
                    [] ->
                        mnesia:write(?USAGE_TAB, Usage1, write);
                    [TotalUsage0] ->
                        TotalUsage = update_usage_record(Usage1, TotalUsage0),
                        mnesia:write(?USAGE_TAB, TotalUsage, write)
                end
            end)
    end,
    TBuffer = {Usage, now_ts()},
    {reply, ok, State#{buffer := Buffer#{TenantId => TBuffer}}};
handle_call({remove, TenantId}, _From, State = #{buffer := Buffer}) ->
    NBuffer = maps:remove(TenantId, Buffer),
    ok = trans(fun() -> mnesia:delete(?USAGE_TAB, TenantId, write) end),
    {reply, ok, State#{buffer := NBuffer}};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(
    {acquired, TenantId, Resource, Taker},
    State = #{pmon := PMon, buffer := Buffer}
) ->
    PMon1 =
        case Resource of
            connections ->
                emqx_pmon:monitor(Taker, {TenantId, Resource}, PMon);
            _ ->
                PMon
        end,
    {Usage, LastSubmitTs} = maps:get(TenantId, Buffer),
    TBuffer = {incr(Resource, 1, Usage), LastSubmitTs},
    Buffer1 = Buffer#{TenantId => TBuffer},
    {noreply, submit(State#{pmon := PMon1, buffer := Buffer1})};
handle_cast({released, TenantId, Resource}, State = #{buffer := Buffer}) ->
    Buffer1 = do_release(TenantId, Resource, Buffer),
    {noreply, submit(State#{buffer := Buffer1})}.

handle_info(submit, State) ->
    {noreply, ensure_submit_timer(submit(State))};
handle_info({'DOWN', _MRef, process, Pid, _Reason}, State = #{pmon := PMon, buffer := Buffer}) ->
    ChanPids = [Pid | emqx_misc:drain_down(?BATCH_SIZE)],
    {Items, PMon1} = emqx_pmon:erase_all(ChanPids, PMon),
    Buffer1 = lists:foldl(
        fun({_, {TenantId, Resource}}, Acc) ->
            do_release(TenantId, Resource, Acc)
        end,
        Buffer,
        Items
    ),
    {noreply, submit(State#{pmon := PMon1, buffer := Buffer1})};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% submit

ensure_submit_timer(State = #{delayed_submit_ms := Interval}) when Interval > 0 ->
    erlang:send_after(Interval, self(), submit),
    State;
ensure_submit_timer(State) ->
    State.

submit(
    State = #{
        buffer := Buffer,
        delayed_submit_ms := Interval,
        delayed_submit_diff := AllowedDiff
    }
) ->
    Now = now_ts(),
    %% return true if it should be submit
    Pred = fun(_TenantId, {Usage, LastSubmitTs}) ->
        case max_abs_diff(Usage) of
            Diff when Diff == 0 -> false;
            Diff when Diff >= AllowedDiff -> true;
            _ -> (Now - LastSubmitTs) > Interval
        end
    end,
    case maps:filter(Pred, Buffer) of
        Need when map_size(Need) =:= 0 ->
            State;
        Need ->
            Need1 = do_submit(Now, Need),
            State#{buffer := maps:merge(Buffer, Need1)}
    end.

max_abs_diff(Usage) when is_record(Usage, usage) ->
    Fun = fun(I, Max) ->
        #{used := Diff0} = element(I, Usage),
        Diff = abs(Diff0),
        case Diff > Max of
            true ->
                Diff;
            false ->
                Max
        end
    end,
    lists:foldl(Fun, 0, lists:seq(3, record_info(size, usage))).

do_submit(Now, Need) ->
    trans(
        fun() ->
            maps:foreach(
                fun(TenantId, {Usage, _}) ->
                    case mnesia:read(?USAGE_TAB, TenantId, write) of
                        [] ->
                            mnesia:write(?USAGE_TAB, Usage, write);
                        [Usage0] ->
                            Usage1 = update_usage_record(Usage0, Usage),
                            mnesia:write(?USAGE_TAB, Usage1, write)
                    end
                end,
                Need
            )
        end
    ),
    maps:map(
        fun(_, {Usage, _}) ->
            Usage1 = lists:foldl(
                fun(I, Acc) ->
                    C = element(I, Acc),
                    setelement(I, Acc, C#{used := 0})
                end,
                Usage,
                lists:seq(3, record_info(size, usage))
            ),
            {Usage1, Now}
        end,
        Need
    ).

%%--------------------------------------------------------------------
%% clear

do_release(TenantId, Resource, Buffer) ->
    case maps:get(TenantId, Buffer, undefined) of
        undefined ->
            Buffer;
        {Usage, LastSubmitTs} ->
            TBuffer = {incr(Resource, -1, Usage), LastSubmitTs},
            Buffer#{TenantId => TBuffer}
    end.

%%--------------------------------------------------------------------
%% helpers

trans(Fun) ->
    case mria:transaction(?COMMON_SHARD, Fun) of
        {atomic, ok} -> ok;
        {atomic, Res} -> {ok, Res};
        {aborted, Error} -> {error, Error}
    end.

%% @doc use New's max, and Old's + New's used
update_usage_record(New, Old) when is_record(New, usage), is_record(Old, usage) ->
    lists:foldl(
        fun(I, Acc) ->
            #{max := Max, used := Used0} = element(I, New),
            #{used := Used1} = element(I, Old),
            setelement(I, Acc, #{max => Max, used => Used0 + Used1})
        end,
        New,
        lists:seq(3, record_info(size, usage))
    ).

config_to_usage(
    TenantId,
    #{
        max_connection := MaxConns,
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
        subs = counter(MaxSubs),
        retained = counter(MaxRetained),
        rules = counter(MaxRules),
        resources = counter(MaxRes),
        shared_subs = counter(MaxSharedSub)
    }.

usage_to_info(#usage{
    connections = Conns,
    authn_users = AuthN,
    authz_users = AuthZ,
    subs = Subs,
    retained = Retained,
    rules = Rules,
    resources = Res,
    shared_subs = SharedSub
}) ->
    #{
        connections => Conns,
        authn_users => AuthN,
        authz_users => AuthZ,
        subs => Subs,
        retained => Retained,
        rules => Rules,
        resources => Res,
        shared_subs => SharedSub
    }.

counter(Max) ->
    #{max => Max, used => 0}.

incr(connections, N, Usage = #usage{connections = C = #{used := Used}}) ->
    Usage#usage{connections = C#{used := Used + N}};
incr(authn_users, N, Usage = #usage{authn_users = C = #{used := Used}}) ->
    Usage#usage{authn_users = C#{used := Used + N}};
incr(authz_users, N, Usage = #usage{authz_users = C = #{used := Used}}) ->
    Usage#usage{authz_users = C#{used := Used + N}};
incr(subs, N, Usage = #usage{subs = C = #{used := Used}}) ->
    Usage#usage{subs = C#{used := Used + N}};
incr(retained, N, Usage = #usage{retained = C = #{used := Used}}) ->
    Usage#usage{retained = C#{used := Used + N}};
incr(rules, N, Usage = #usage{rules = C = #{used := Used}}) ->
    Usage#usage{rules = C#{used := Used + N}};
incr(resources, N, Usage = #usage{resources = C = #{used := Used}}) ->
    Usage#usage{resources = C#{used := Used + N}};
incr(shared_subs, N, Usage = #usage{shared_subs = C = #{used := Used}}) ->
    Usage#usage{shared_subs = C#{used := Used + N}}.

now_ts() ->
    erlang:system_time(millisecond).
