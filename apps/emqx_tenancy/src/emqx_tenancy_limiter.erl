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
            BucketId = emqx_tenancy_limiter_server:bucket_id(TenantId, Type),
            emqx_htb_limiter:connect_to_extra(BucketId, Type, Limiter)
        end,
        Limiters
    ),
    {ok, NLimiters}.

%%--------------------------------------------------------------------
%% management APIs

%% @doc create all limiters for a tenant
-spec create(tenant_id(), limiter_config()) -> ok | {error, term()}.
create(TenantId, Config) ->
    %% TODO: 0. How to sync to all nodes
    emqx_tenancy_limiter_sup:create(TenantId, Config).

%% @doc update all limiters for a tenant
-spec update(tenant_id(), limiter_config()) -> ok | {error, term()}.
update(TenantId, Config) ->
    %% TODO: clustering?
    emqx_tenancy_limiter_sup:update(TenantId, Config).

%% @doc remove a tenant's all limiters
-spec remove(tenant_id()) -> ok.
remove(TenantId) ->
    %% TODO: clustering?
    emqx_tenancy_limiter_sup:remove(TenantId).

%% @doc lookup all limiters info of a tenant in current node
-spec info(tenant_id()) -> {ok, [limiter_info()]} | {error, term()}.
info(TenantId) ->
    emqx_tenancy_limiter_sup:info(TenantId).
