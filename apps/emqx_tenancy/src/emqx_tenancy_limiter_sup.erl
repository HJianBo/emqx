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

-module(emqx_tenancy_limiter_sup).

-include("emqx_tenancy.hrl").

-behaviour(supervisor).

%% API
-export([start_link/0]).

-export([
    create/2,
    update/2,
    remove/1,
    info/1
]).

%% Supervisor callbacks
-export([init/1]).

-define(PID, whereis(emqx_tenancy_limiter_server)).

%%--------------------------------------------------------------------
%% APIs

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec create(tenant_id(), limiter_config()) -> ok | {error, term()}.
create(TenantId, Config) ->
    emqx_tenancy_limiter_server:create(?PID, TenantId, Config).

-spec update(tenant_id(), limiter_config()) -> ok | {error, term()}.
update(TenantId, Config) ->
    emqx_tenancy_limiter_server:update(?PID, TenantId, Config).

-spec remove(tenant_id()) -> ok.
remove(TenantId) ->
    emqx_tenancy_limiter_server:remove(?PID, TenantId).

-spec info(tenant_id()) -> {ok, [limiter_info()]} | {error, term()}.
info(TenantId) ->
    emqx_tenancy_limiter_server:info(?PID, TenantId).

%%--------------------------------------------------------------------
%% callbacks

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },
    Server = #{
        id => emqx_tenancy_limiter_server,
        type => worker,
        start => {emqx_tenancy_limiter_server, start_link, []},
        restart => permanent,
        shutdown => 5000
    },
    Storage = #{
        id => emqx_tenancy_limiter_storage,
        type => worker,
        start => {emqx_tenancy_limiter_storage, start_link, []},
        restart => permanent,
        shutdown => 5000
    },
    {ok, {SupFlags, [Storage, Server]}}.
