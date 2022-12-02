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

-module(emqx_tenancy_limiter_server_sup).

-include("emqx_tenancy.hrl").

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%%--------------------------------------------------------------------
%% APIs

start_link(Config) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Config]).

init([Config]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },
    Children = children_from_config(maps:to_list(Config), []),
    {ok, {SupFlags, Children}}.

children_from_config([], Acc) ->
    Acc;
children_from_config([{Key, Rate} | L], Acc) ->
    Type = type(Key),
    Spec = #{
        id => Key,
        %% TODO:
        start => {emqx_limiter_server, start_link, [Type, #{rate => Rate / 10, burst => 0}]},
        restart => permanent,
        shutdown => 5000
    },
    children_from_config(L, [Spec | Acc]).

type(max_conn_rate) -> connection;
type(max_messages_in) -> message_in;
type(max_bytes_in) -> bytes_in.
%% TODO: max_sub_in?
