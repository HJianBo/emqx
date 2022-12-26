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
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_server).

%% APIs
-export([start_link/1, stop/0]).
-export([load/0, unload/0]).

%% Management APIs
-export([create/2, update/2, remove/1, info/1]).

%% Node level APIs
-export([do_create/2, do_update/2, do_remove/1, do_info/1]).

-define(USAGE_TAB, emqx_tenancy_usage).

-define(DEFAULT_SUBMIT_DELAY, 100).
-define(DEFAULT_SUBMIT_DIFF, 10).
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
    on_quota_sessions/3,
    on_quota_authn_users/3,
    on_quota_authz_rules/3
]).
-export([acquire/3, release/3]).

%% for tests
-export([is_tenant_enabled/1]).

-type state() :: #{
    %% 0 means sync to mnesia immediately
    delayed_submit_ms := non_neg_integer(),
    %% How much delayed count is allowed
    delayed_submit_diff := non_neg_integer(),
    %% local schema and buffer
    buffer := #{tenant_id() => {usage(), LastSubmitTs :: pos_integer()}}
}.

-type options() :: #{
    delayed_submit_ms => non_neg_integer(),
    delayed_submit_diff => non_neg_integer()
}.

-record(usage, {
    id :: tenant_id(),
    sessions :: usage_counter(),
    authn_users :: usage_counter(),
    authz_rules :: usage_counter(),
    subs :: usage_counter(),
    retained :: usage_counter(),
    rules :: usage_counter(),
    resources :: usage_counter(),
    shared_subs :: usage_counter()
}).

-type usage() :: #usage{}.

-type usage_info() :: #{
    sessions := usage_counter(),
    authn_users := usage_counter(),
    authz_rules := usage_counter(),
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

-spec start_link(options()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Options) ->
    Def = #{
        delayed_submit_ms => ?DEFAULT_SUBMIT_DELAY,
        delayed_submit_diff => ?DEFAULT_SUBMIT_DIFF
    },
    NOptions = maps:merge(Def, Options),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [NOptions], []).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?MODULE).

%%--------------------------------------------------------------------
%% load/unload

%% @doc Load tenancy limiter
%% It's only works for new sessions
-spec load() -> ok.
load() ->
    emqx_hooks:put('quota.sessions', {?MODULE, on_quota_sessions, []}, 0),
    emqx_hooks:put('quota.authn_users', {?MODULE, on_quota_authn_users, []}, 0),
    emqx_hooks:put('quota.authz_rules', {?MODULE, on_quota_authz_rules, []}, 0).

%% @doc Unload tenant limiter
%% It's only works for new sessions
-spec unload() -> ok.
unload() ->
    emqx_hooks:del('quota.sessions', {?MODULE, on_quota_sessions}),
    emqx_hooks:del('quota.authn_users', {?MODULE, on_quota_authn_users}),
    emqx_hooks:del('quota.authz_rules', {?MODULE, on_quota_authz_rules}).

%%--------------------------------------------------------------------
%% Management APIs (cluster-level)

-spec create(tenant_id(), quota_config()) -> ok.
create(TenantId, Config) ->
    %% FIXME: atomicity?
    multicall(?MODULE, do_create, [TenantId, Config]).

-spec update(tenant_id(), quota_config()) -> ok.
update(TenantId, Config) ->
    %% FIXME: atomicity?
    multicall(?MODULE, do_update, [TenantId, Config]).

-spec remove(tenant_id()) -> ok.
remove(TenantId) ->
    multicall(?MODULE, do_remove, [TenantId]).

-spec info(tenant_id()) -> {ok, usage_info()} | {error, term()}.
info(TenantId) ->
    do_info(TenantId).

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
    call({info, TenantId}).

call(Msg) ->
    gen_server:call(?MODULE, Msg).

cast(Msg) ->
    gen_server:cast(?MODULE, Msg).

%%--------------------------------------------------------------------
%% Hooks Callback

-type quota_action_name() :: acquire | release.

-type quota_action() :: quota_action_name() | {quota_action_name(), pos_integer()}.

-type permision() :: {stop, allow | deny}.

-spec on_quota_sessions(quota_action(), emqx_types:clientinfo(), term()) -> permision().
on_quota_sessions(_Action, #{tenant_id := ?NO_TENANT}, _LastPermision) ->
    {stop, allow};
on_quota_sessions(Action, ClientInfo = #{tenant_id := TenantId}, _LastPermision) ->
    case is_acquire_action(Action) of
        true ->
            case ?MODULE:is_tenant_enabled(TenantId) of
                true ->
                    Res = exec_quota_action(Action, [TenantId, sessions]),
                    case Res of
                        {stop, allow} ->
                            ClientId = maps:get(clientid, ClientInfo),
                            emqx_tenancy_resm:monitor_session_proc(
                                self(),
                                TenantId,
                                ClientId
                            );
                        _ ->
                            ok
                    end,
                    Res;
                false ->
                    io:format("disabled failed\n"),
                    {stop, deny}
            end;
        false ->
            exec_quota_action(Action, [TenantId, sessions])
    end.

is_tenant_enabled(TenantId) ->
    case ets:lookup(?TENANCY, TenantId) of
        [] -> false;
        [#tenant{enabled = Enabled}] -> Enabled == true
    end.

-spec on_quota_authn_users(quota_action(), tenant_id(), term()) -> permision().
on_quota_authn_users(_Action, ?NO_TENANT, _LastPermision) ->
    {stop, allow};
on_quota_authn_users(Action, TenantId, _LastPermision) ->
    exec_quota_action(Action, [TenantId, authn_users]).

-spec on_quota_authz_rules(quota_action(), tenant_id(), term()) -> permision().
on_quota_authz_rules(_Action, ?NO_TENANT, _LastPermision) ->
    {stop, allow};
on_quota_authz_rules(Action, TenantId, _LastPermision) ->
    exec_quota_action(Action, [TenantId, authz_rules]).

exec_quota_action(Action, Args) when Action == acquire; Action == release ->
    erlang:apply(?MODULE, Action, [1 | Args]);
exec_quota_action({Action, N}, Args) when Action == acquire; Action == release ->
    erlang:apply(?MODULE, Action, [N | Args]).

is_acquire_action(acquire) -> true;
is_acquire_action({acquire, _}) -> true;
is_acquire_action(_) -> false.

acquire(N, TenantId, Resource) ->
    case call({acquire, N, TenantId, Resource}) of
        allow ->
            {stop, allow};
        deny ->
            {stop, deny}
    end.

release(N, TenantId, Resource) ->
    try ets:lookup_element(?USAGE_TAB, TenantId, position(Resource)) of
        _ ->
            cast({released, N, TenantId, Resource}),
            {stop, allow}
    catch
        error:badarg ->
            {stop, deny}
    end.

position(Resource) ->
    P = #{
        sessions => #usage.sessions,
        authn_users => #usage.authn_users,
        authz_rules => #usage.authz_rules,
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

-spec init([options()]) -> {ok, state()}.
init([Options]) ->
    process_flag(trap_exit, true),
    ok = mria:create_table(?USAGE_TAB, [
        {type, set},
        {rlog_shard, ?TENANCY_SHARD},
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
        delayed_submit_ms => maps:get(delayed_submit_ms, Options),
        delayed_submit_diff => maps:get(delayed_submit_diff, Options),
        buffer => load_tenants_used()
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
handle_call({info, TenantId}, _From, State = #{buffer := Buffer}) ->
    Reply =
        case ets:lookup(?USAGE_TAB, TenantId) of
            [] ->
                {error, not_found};
            [Usage0] ->
                {Usage1, _} = maps:get(TenantId, Buffer),
                TotalUsage = update_usage_record(Usage0, Usage1),
                {ok, usage_to_info(TotalUsage)}
        end,
    {reply, Reply, State};
handle_call(
    {acquire, N, TenantId, Resource},
    _From,
    State = #{buffer := Buffer}
) ->
    {Reply, Buffer1} = do_acquire(N, TenantId, Resource, Buffer),
    {reply, Reply, submit(State#{buffer := Buffer1})}.

handle_cast({released, N, TenantId, Resource}, State = #{buffer := Buffer}) ->
    Buffer1 = do_release(N, TenantId, Resource, Buffer),
    {noreply, submit(State#{buffer := Buffer1})}.

handle_info(submit, State) ->
    {noreply, ensure_submit_timer(submit(State))};
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
%% load

load_tenants_used() ->
    AuthNUsed = scan_authn_users_table(),
    AuthZUsed = scan_authz_rules_table(),
    RetainedUsed = scan_retained_table(),
    lists:foldl(
        fun(#tenant{id = Id, configs = Configs}, Acc) ->
            Usage = config_to_usage(
                Id,
                emqx_tenancy:with_quota_config(Configs)
            ),
            Usage1 = incr(authn_users, maps:get(Id, AuthNUsed, 0), Usage),
            Usage2 = incr(authz_rules, maps:get(Id, AuthZUsed, 0), Usage1),
            Usage3 = incr(retained, maps:get(Id, RetainedUsed, 0), Usage2),
            ok = trans(fun() -> mnesia:write(?USAGE_TAB, Usage3, write) end),
            Acc#{Id => {Usage, 0}}
        end,
        #{},
        ets:tab2list(?TENANCY)
    ).

%% XXX: how to optimize?
scan_authn_users_table() ->
    ets:foldl(
        fun({user_info, {_Group, TenantId, _UserId}, _, _, _}, Acc) ->
            N = maps:get(TenantId, Acc, 0),
            Acc#{TenantId => N + 1}
        end,
        #{},
        emqx_authn_mnesia
    ).

scan_authz_rules_table() ->
    ets:foldl(
        fun({emqx_acl, Who, Rules}, Acc) ->
            Id =
                case Who of
                    {_, Id0} -> Id0;
                    {_, Id0, _} -> Id0
                end,
            N = maps:get(Id, Acc, 0),
            Acc#{Id => N + length(Rules)}
        end,
        #{},
        emqx_acl
    ).

scan_retained_table() ->
    %% TODO: 2.0
    #{}.

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
    NState =
        case maps:filter(Pred, Buffer) of
            Need when map_size(Need) =:= 0 ->
                State;
            Need ->
                Need1 = do_submit(Now, Need),
                State#{buffer := maps:merge(Buffer, Need1)}
        end,
    ?tp(debug, submit, #{}),
    NState.

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
    ok = trans(
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
%% acquire & release

do_acquire(N, TenantId, Resource, Buffer) ->
    case maps:get(TenantId, Buffer, undefined) of
        undefined ->
            {deny, Buffer};
        {Usage, LastSubmitTs} ->
            Pos = position(Resource),
            #{max := Max, used := Used0} = ets:lookup_element(?USAGE_TAB, TenantId, Pos),
            #{used := Used} = element(Pos, Usage),
            case Max >= Used0 + Used + N of
                true ->
                    TBuffer = {incr(Resource, N, Usage), LastSubmitTs},
                    {allow, Buffer#{TenantId => TBuffer}};
                false ->
                    {deny, Buffer}
            end
    end.

do_release(N, TenantId, Resource, Buffer) ->
    case maps:get(TenantId, Buffer, undefined) of
        undefined ->
            Buffer;
        {Usage, LastSubmitTs} ->
            TBuffer = {incr(Resource, -N, Usage), LastSubmitTs},
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
        max_sessions := MaxSess,
        max_authn_users := MaxAuthN,
        max_authz_rules := MaxAuthZ
    }
) ->
    #usage{
        id = TenantId,
        sessions = counter(MaxSess),
        authn_users = counter(MaxAuthN),
        authz_rules = counter(MaxAuthZ),
        subs = counter(infinity),
        retained = counter(infinity),
        rules = counter(infinity),
        resources = counter(infinity),
        shared_subs = counter(infinity)
    }.

usage_to_info(#usage{
    sessions = Sess,
    authn_users = AuthN,
    authz_rules = AuthZ,
    subs = Subs,
    retained = Retained,
    rules = Rules,
    resources = Res,
    shared_subs = SharedSub
}) ->
    #{
        sessions => Sess,
        authn_users => AuthN,
        authz_rules => AuthZ,
        subs => Subs,
        retained => Retained,
        rules => Rules,
        resources => Res,
        shared_subs => SharedSub
    }.

counter(Max) ->
    #{max => Max, used => 0}.

incr(sessions, N, Usage = #usage{sessions = C = #{used := Used}}) ->
    Usage#usage{sessions = C#{used := Used + N}};
incr(authn_users, N, Usage = #usage{authn_users = C = #{used := Used}}) ->
    Usage#usage{authn_users = C#{used := Used + N}};
incr(authz_rules, N, Usage = #usage{authz_rules = C = #{used := Used}}) ->
    Usage#usage{authz_rules = C#{used := Used + N}};
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
