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

-export([create/2]).

%% top-level APIs for tenancy Mgmt

-spec create(tenant_id(), limiter_config()) -> supervisor:startchild_ret().
create(TenantId, Config) ->
    %% TODO: 0. How to sync to all nodes
    emqx_tenancy_limiter_sup:create(TenantId, Config).

%-spec update(tenant_id(), limiter_config()) -> ok.
%
%-spec remove(tenant_id()) -> ok.
%
%%-spec lookup(tenant_id()) -> {error, not_found} | {ok, usage()}.
%%
%%-spec list(()) -> [usage()].
%
%%% APIs for consumer
%
%%% checkout all matched limiters
%-spec get_limiter_by_types(
%    tenant_id(),
%    [emqx_limiter_container:limiter_type()]
%) -> emqx_limiter_container:container().
%
%%% check list of limiters
%-spec check_list(
%    [{pos_integer(), emqx_limiter_container:limiter_type()}],
%    emqx_limiter_container:container()
%) -> emqx_limiter_container:check_result().
