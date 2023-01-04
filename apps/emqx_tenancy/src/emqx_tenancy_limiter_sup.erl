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

%%--------------------------------------------------------------------
%% APIs

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec create(tenant_id(), limiter_config()) -> ok | {error, term()}.
create(TenantId, Config) ->
    case start_child(TenantId, Config) of
        {ok, _Pid} ->
            ok;
        {error, _} = Err ->
            Err
    end.

-spec update(tenant_id(), limiter_config()) -> ok | {error, term()}.
update(TenantId, Config) ->
    case find_sup_child(?MODULE, name(TenantId)) of
        false ->
            {error, not_found};
        {ok, Pid} ->
            emqx_tenancy_limiter_server:update(Pid, Config)
    end.

-spec remove(tenant_id()) -> ok.
remove(TenantId) ->
    Name = name(TenantId),
    case find_sup_child(?MODULE, Name) of
        false ->
            ok;
        {ok, _Pid} ->
            _ = supervisor:terminate_child(?MODULE, Name),
            _ = supervisor:delete_child(?MODULE, Name),
            ok
    end.

-spec info(tenant_id()) -> {ok, [limiter_info()]} | {error, term()}.
info(TenantId) ->
    case find_sup_child(?MODULE, name(TenantId)) of
        false -> {error, not_found};
        {ok, Pid} -> {ok, emqx_tenancy_limiter_server:info(Pid)}
    end.

%%--------------------------------------------------------------------
%% callbacks

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },
    {ok, {SupFlags, []}}.

%%--------------------------------------------------------------------
%% internal funcs

start_child(TenantId, Config) ->
    Spec = #{
        id => name(TenantId),
        type => worker,
        start => {emqx_tenancy_limiter_server, start_link, [TenantId, Config]},
        restart => permanent,
        shutdown => 5000
    },
    case supervisor:start_child(?MODULE, Spec) of
        {ok, Pid} ->
            {ok, Pid};
        {ok, Pid, _Info} ->
            {ok, Pid};
        {error, {already_started, Pid}} when is_pid(Pid) ->
            ok = emqx_tenancy_limiter_server:update(Pid, Config),
            {ok, Pid};
        {error, _} = Err ->
            Err
    end.

find_sup_child(Sup, ChildId) ->
    case lists:keyfind(ChildId, 1, supervisor:which_children(Sup)) of
        false -> false;
        {_Id, Pid, _Type, _Mods} -> {ok, Pid}
    end.

name(TenantId) ->
    TenantId.
