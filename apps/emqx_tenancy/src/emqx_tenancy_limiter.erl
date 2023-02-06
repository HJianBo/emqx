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

%% Provide node-level APIs for Multi-Tenancy Ratelimit
-module(emqx_tenancy_limiter).

-include("emqx_tenancy.hrl").

%% load/unload
-export([load/0, unload/0]).

%% management APIs
-export([create/2, update/2, remove/1, info/1]).

%% node level APIs
-export([do_create/2, do_update/2, do_remove/1, do_info/1]).

%% hooks
-export([on_upgarde_limiters/2]).

%%--------------------------------------------------------------------
%% load/unload

%% @doc Load tenancy limiter
%% It's only works for new connections
-spec load() -> ok.
load() ->
    emqx_hooks:put('tenant.upgrade_limiters', {?MODULE, on_upgarde_limiters, []}, 0).

%% @doc Unload tenanct limiter
%% It's only works for new connections
-spec unload() -> ok.
unload() ->
    emqx_hooks:del('tenant.upgrade_limiters', {?MODULE, on_upgarde_limiters, []}).

%%--------------------------------------------------------------------
%% hooks

-spec on_upgarde_limiters(tenant_id(), Limiters) -> {ok, Limiters} when
    Limiters :: #{limiter_type() => emqx_htb_limiter:limiter()}.
on_upgarde_limiters(TenantId, Limiters) ->
    NLimiters = maps:map(
        fun(Type, Limiter) ->
            BucketId = emqx_tenancy_limiter_server:bucket_id(TenantId),
            emqx_htb_limiter:connect_to_extra(BucketId, Type, Limiter)
        end,
        Limiters
    ),
    {ok, NLimiters}.

%%--------------------------------------------------------------------
%% Management APIs (cluster-level)

%% @doc create all limiters for a tenant
-spec create(tenant_id(), limiter_config()) -> ok | {error, term()}.
create(TenantId, Config) ->
    multicall(?MODULE, do_create, [TenantId, Config]).

%% @doc update all limiters for a tenant
-spec update(tenant_id(), limiter_config()) -> ok | {error, term()}.
update(TenantId, Config) ->
    multicall(?MODULE, do_update, [TenantId, Config]).

%% @doc remove a tenant's all limiters
-spec remove(tenant_id()) -> ok.
remove(TenantId) ->
    multicall(?MODULE, do_remove, [TenantId]).

%% @doc lookup all limiters info of a tenant in current node
-spec info(tenant_id()) -> {ok, #{node() => limiter_info()}} | {error, term()}.
info(TenantId) ->
    Nodes = mria_mnesia:running_nodes(),
    case emqx_rpc:multicall(Nodes, ?MODULE, do_info, [TenantId]) of
        {Result, []} ->
            NodesResult = lists:zip(Nodes, Result),
            case return_ok_or_error(NodesResult) of
                ok ->
                    lists:foldl(
                        fun({Node, Info}, Acc) ->
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

do_create(TenantId, Config) ->
    emqx_tenancy_limiter_sup:create(TenantId, Config).

do_update(TenantId, Config) ->
    emqx_tenancy_limiter_sup:update(TenantId, Config).

do_remove(TenantId) ->
    emqx_tenancy_limiter_sup:remove(TenantId).

do_info(TenantId) ->
    emqx_tenancy_limiter_sup:info(TenantId).
