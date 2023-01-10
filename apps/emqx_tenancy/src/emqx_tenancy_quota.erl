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

%% Mnesia
-export([mnesia/1]).
-boot_mnesia({mnesia, [boot]}).

%% APIs
-export([start_link/1, stop/0]).
-export([load/0, unload/0]).

%% Management APIs
-export([create/2, update/2, remove/1, info/1]).

-export([cleanup_node_down/1]).

%% Node level APIs
-export([do_create/2, do_update/2, do_remove/1, do_info/1]).

-define(COUNTER, emqx_tenancy_quota_counter).

-define(DEFAULT_SUBMIT_DELAY, 100).

-define(DEFAULT_SUBMIT_DIFF, 10).

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
    %% local buffer
    buffer := #{tenant_id() => {usage(), LastSubmitTs :: pos_integer()}},
    %%
    await := sets:set()
}.

-type options() :: #{
    delayed_submit_ms => non_neg_integer(),
    delayed_submit_diff => non_neg_integer()
}.

-type usage() :: #{
    sessions := usage_counter(),
    authn_users := usage_counter(),
    authz_rules := usage_counter(),
    atom() => usage_counter()
}.

-type usage_info() :: #{
    sessions := usage_counter(),
    authn_users := usage_counter(),
    authz_rules := usage_counter(),
    atom() => usage_counter()
    %% XXX: 2.0
    %%subs := usage_counter(),
    %%retained := usage_counter(),
    %%rules := usage_counter(),
    %%resources := usage_counter(),
    %%shared_subs := usage_counter()
}.

-type usage_counter() :: #{max := pos_integer(), used := non_neg_integer()}.

%%--------------------------------------------------------------------
%% Mnesia
%%--------------------------------------------------------------------

-spec mnesia(boot) -> ok.
mnesia(boot) ->
    ok = mria:create_table(?COUNTER, [
        {type, set},
        {rlog_shard, ?TENANCY_SHARD},
        {storage, ram_copies},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]).

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
%% Node down

-spec cleanup_node_down(node()) -> ok.
cleanup_node_down(Node) ->
    Ms = [{{'_', {'$1', sessions, '$2'}, '$3'}, [{'=:=', '$2', Node}], [{{'$1', '$3'}}]}],
    Ls = ets:select(?COUNTER, Ms),
    lists:foreach(
        fun({TenantId, Cnt}) ->
            mria:dirty_update_counter(?COUNTER, {TenantId, sessions}, -Cnt),
            mria:dirty_update_counter(?COUNTER, {TenantId, sessions, Node}, -Cnt)
        end,
        Ls
    ).

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

-spec on_quota_sessions(quota_action(), map(), term()) -> permision().
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
    try unsafe_lookup_counter(TenantId, Resource) of
        _ ->
            cast({released, N, TenantId, Resource})
    catch
        error:badarg -> ok
    end,
    %% Note: return `allow` regardless of whether the resource exists
    %% or it is sufficiently to free
    {stop, allow}.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

-spec init([options()]) -> {ok, state()}.
init([Options]) ->
    process_flag(trap_exit, true),
    State = #{
        delayed_submit_ms => maps:get(delayed_submit_ms, Options),
        delayed_submit_diff => maps:get(delayed_submit_diff, Options),
        buffer => load_tenants_used(),
        await => sets:new()
    },
    {ok, ensure_submit_timer(State)}.

handle_call(
    {create, TenantId, Config},
    _From,
    State = #{buffer := Buffer}
) ->
    Usage = init_usage(Config),
    ok = init_counter(TenantId),
    TBuffer = {Usage, now_ts()},
    {reply, ok, State#{buffer := Buffer#{TenantId => TBuffer}}};
handle_call({remove, TenantId}, _From, State = #{buffer := Buffer, await := Await}) ->
    NBuffer = maps:remove(TenantId, Buffer),
    NAwait = sets:del_element(TenantId, Await),
    ok = clear_counter(TenantId),
    {reply, ok, State#{buffer := NBuffer, await := NAwait}};
handle_call({info, TenantId}, _From, State = #{buffer := Buffer}) ->
    Reply =
        case maps:get(TenantId, Buffer, undefined) of
            undefined ->
                {error, not_found};
            {Usage, _} ->
                {ok, apply_counter_to_usage(TenantId, Usage)}
        end,
    {reply, Reply, State};
handle_call(
    {acquire, N, TenantId, Resource},
    _From,
    State = #{buffer := Buffer}
) ->
    {Reply, Buffer1} = do_acquire(N, TenantId, Resource, Buffer),
    {reply, Reply, submit(TenantId, State#{buffer := Buffer1})}.

handle_cast({released, N, TenantId, Resource}, State = #{buffer := Buffer}) ->
    Buffer1 = do_release(N, TenantId, Resource, Buffer),
    {noreply, submit(TenantId, State#{buffer := Buffer1})}.

handle_info(submit, State = #{await := Await}) ->
    case sets:is_empty(Await) of
        true ->
            {noreply, ensure_submit_timer(State)};
        false ->
            {noreply, ensure_submit_timer(submit(State))}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    _ = mnesia:clear_table(?COUNTER),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% counter

update_counter(TenantId, sessions, N) ->
    mria:dirty_update_counter(?COUNTER, {TenantId, sessions}, N),
    mria:dirty_update_counter(?COUNTER, {TenantId, sessions, node()}, N);
update_counter(TenantId, Resource, N) ->
    mria:dirty_update_counter(?COUNTER, {TenantId, Resource}, N).

lookup_counter(TenantId, Resource) ->
    try
        unsafe_lookup_counter(TenantId, Resource)
    catch
        error:badarg -> 0
    end.

unsafe_lookup_counter(TenantId, Resource) ->
    ets:lookup_element(?COUNTER, {TenantId, Resource}, 3).

init_counter(TenantId) ->
    mria:dirty_update_counter(?COUNTER, {TenantId, sessions}, 0),
    mria:dirty_update_counter(?COUNTER, {TenantId, authn_users}, 0),
    mria:dirty_update_counter(?COUNTER, {TenantId, authz_rules}, 0),
    ok.

clear_counter(TenantId) ->
    ok = mnesia:dirty_delete(?COUNTER, {TenantId, sessions}),
    ok = mnesia:dirty_delete(?COUNTER, {TenantId, authn_users}),
    ok = mnesia:dirty_delete(?COUNTER, {TenantId, authz_rules}).

%%--------------------------------------------------------------------
%% load

load_tenants_used() ->
    AuthNUsed = scan_authn_users_table(),
    AuthZUsed = scan_authz_rules_table(),
    NowTs = now_ts(),
    lists:foldl(
        fun(#tenant{id = Id, configs = Configs}, Acc) ->
            Usage = init_usage(emqx_tenancy:with_quota_config(Configs)),
            update_counter(Id, sessions, 0),
            update_counter(Id, authn_users, maps:get(Id, AuthNUsed, 0)),
            update_counter(Id, authz_rules, maps:get(Id, AuthZUsed, 0)),
            Acc#{Id => {Usage, NowTs}}
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

%%--------------------------------------------------------------------
%% submit

ensure_submit_timer(State = #{delayed_submit_ms := Interval}) when Interval > 0 ->
    erlang:send_after(Interval, self(), submit),
    State;
ensure_submit_timer(State) ->
    State.

submit(
    State = #{
        await := Await,
        buffer := Buffer,
        delayed_submit_ms := Interval,
        delayed_submit_diff := AllowedDiff
    }
) ->
    Now = now_ts(),
    %% return if it should be submit
    Pred =
        fun(TenantId, Acc) ->
            {Usage, LastSubmitTs} = maps:get(TenantId, Buffer),
            Bool =
                case max_abs_diff(Usage) of
                    Diff when Diff == 0 -> false;
                    Diff when Diff >= AllowedDiff -> true;
                    _ -> (Now - LastSubmitTs) > Interval
                end,
            case Bool of
                false -> Acc;
                true -> Acc#{TenantId => {Usage, LastSubmitTs}}
            end
        end,
    NState =
        case sets:fold(Pred, #{}, Await) of
            Need when map_size(Need) =:= 0 ->
                State;
            Need ->
                Need1 = maps:map(
                    fun(TenantId, {Usage, _}) ->
                        Usage1 = do_submit(TenantId, Usage),
                        {Usage1, Now}
                    end,
                    Need
                ),
                ?tp(debug, submit, #{tenants_count => map_size(Need)}),
                Await1 = lists:foldl(
                    fun(TenantId, Acc) -> sets:del_element(TenantId, Acc) end,
                    Await,
                    maps:keys(Need1)
                ),
                State#{buffer := maps:merge(Buffer, Need1), await := Await1}
        end,
    NState.

submit(
    TenantId,
    State = #{
        await := Await,
        buffer := Buffer,
        delayed_submit_ms := Interval,
        delayed_submit_diff := AllowedDiff
    }
) ->
    case maps:get(TenantId, Buffer, undefined) of
        {Usage, LastSubmitTs} ->
            Now = now_ts(),
            case
                max_abs_diff(Usage) >= AllowedDiff orelse
                    (Now - LastSubmitTs) > Interval
            of
                true ->
                    Usage1 = do_submit(TenantId, Usage),
                    ?tp(debug, submit, #{tenants_count => 1, tenant_id => TenantId}),
                    State#{buffer := Buffer#{TenantId := {Usage1, Now}}};
                false ->
                    State#{await => sets:add_element(TenantId, Await)}
            end;
        _ ->
            State#{await => sets:add_element(TenantId, Await)}
    end.

max_abs_diff(Usage) ->
    Fun = fun(Resource, Max) ->
        #{used := Diff0} = maps:get(Resource, Usage),
        Diff = abs(Diff0),
        case Diff > Max of
            true ->
                Diff;
            false ->
                Max
        end
    end,
    lists:foldl(Fun, 0, [sessions, authn_users, authz_rules]).

do_submit(TenantId, Usage) ->
    maps:map(
        fun
            (Resource, Counter = #{used := Used}) when Used =/= 0 ->
                update_counter(TenantId, Resource, Used),
                Counter#{used := 0};
            (_, Counter) ->
                Counter
        end,
        Usage
    ).

%%--------------------------------------------------------------------
%% acquire & release

do_acquire(N, TenantId, Resource, Buffer) ->
    try
        {Usage, LastSubmitTs} = maps:get(TenantId, Buffer),
        #{max := Max, used := Used1} = maps:get(Resource, Usage),
        Used2 = unsafe_lookup_counter(TenantId, Resource),
        case Max >= Used1 + Used2 + N of
            true ->
                TBuffer = {incr(Resource, N, Usage), LastSubmitTs},
                {allow, Buffer#{TenantId => TBuffer}};
            false ->
                {deny, Buffer}
        end
    catch
        error:{badkey, _} ->
            {deny, Buffer};
        error:badarg ->
            ?SLOG(
                warning,
                #{
                    msg => "dataset_gone_when_acquire_token",
                    tenant_id => TenantId,
                    resource => Resource
                }
            ),
            {deny, Buffer}
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

init_usage(#{
    max_sessions := MaxSess,
    max_authn_users := MaxAuthn,
    max_authz_rules := MaxAuthz
}) ->
    #{
        sessions => #{max => MaxSess, used => 0},
        authn_users => #{max => MaxAuthn, used => 0},
        authz_rules => #{max => MaxAuthz, used => 0}
    }.

apply_counter_to_usage(
    TenantId,
    #{
        sessions := #{max := MaxSess, used := Sess},
        authn_users := #{max := MaxAuthn, used := Authn},
        authz_rules := #{max := MaxAuthz, used := Authz}
    }
) ->
    #{
        sessions => #{max => MaxSess, used => Sess + lookup_counter(TenantId, sessions)},
        authn_users => #{max => MaxAuthn, used => Authn + lookup_counter(TenantId, authn_users)},
        authz_rules => #{max => MaxAuthz, used => Authz + lookup_counter(TenantId, authz_rules)}
    }.

incr(Resource, N, Usage) ->
    M = #{used := Used} = maps:get(Resource, Usage),
    Usage#{Resource := M#{used := Used + N}}.

now_ts() ->
    erlang:system_time(millisecond).
