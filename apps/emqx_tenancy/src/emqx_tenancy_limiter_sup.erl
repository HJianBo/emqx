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
-export([
    start_link/0,
    create/2
]).

%% Supervisor callbacks
-export([init/1]).

%%--------------------------------------------------------------------
%% APIs

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec create(tenant_id(), limiter_config()) -> supervisor:startchild_ret().
create(TenantId, Config) ->
    start_child(TenantId, Config).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },
    {ok, {SupFlags, []}}.

start_child(TenantId, Config) ->
    Spec = #{
        id => name(TenantId),
        start => {emqx_tenancy_limiter_server_sup, start_link, [Config]},
        restart => permanent,
        shutdown => 5000
    },
    supervisor:start_child(?MODULE, Spec).

name(TenantId) ->
    TenantId.
