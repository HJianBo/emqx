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

-module(emqx_tenancy_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 5
    },
    ChildSpecs = [
        #{
            id => emqx_tenancy_stats,
            start => {emqx_tenancy_stats, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_tenancy_stats]
        },
        #{
            id => emqx_tenancy_monitor,
            start => {emqx_tenancy_monitor, start_link, []},
            restart => permanent,
            shutdown => 1000,
            type => worker,
            modules => [emqx_tenancy_monitor]
        },
        #{
            id => emqx_tenancy_push,
            start => {emqx_tenancy_push, start_link, []},
            restart => permanent,
            shutdown => 1000,
            type => worker,
            modules => [emqx_tenancy_metric]
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.