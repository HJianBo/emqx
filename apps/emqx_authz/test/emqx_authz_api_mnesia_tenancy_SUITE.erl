%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authz_api_mnesia_tenancy_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(DISABLE_TENANT_HEADER, disable).
-define(TENANT_BAR, "tenant_bar").
-define(TENANT_FOO, "tenant_foo").

-import(
    emqx_dashboard_api_test_helpers,
    [
        uri/1,
        request_t/3,
        request_t/4
    ]
).

%%--------------------------------------------------------------------
%% setup
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_authz, emqx_dashboard],
        fun set_special_configs/1
    ),
    Config.

end_per_suite(_Config) ->
    {ok, _} = emqx:update_config(
        [authorization],
        #{
            <<"no_match">> => <<"allow">>,
            <<"cache">> => #{<<"enable">> => <<"true">>},
            <<"sources">> => []
        }
    ),
    emqx_common_test_helpers:stop_apps([emqx_dashboard, emqx_authz, emqx_conf]),
    ok.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config();
set_special_configs(emqx_authz) ->
    {ok, _} = emqx:update_config([authorization, cache, enable], false),
    {ok, _} = emqx:update_config([authorization, no_match], deny),
    {ok, _} = emqx:update_config(
        [authorization, sources],
        [#{<<"type">> => <<"built_in_database">>}]
    ),
    ok;
set_special_configs(_App) ->
    ok.

%%--------------------------------------------------------------------
%% tests
%%--------------------------------------------------------------------

t_isolation(_) ->
    %% add rules for tenant_foo
    {ok, 204, _} =
        request_t(
            post,
            ?TENANT_FOO,
            uri(["authorization", "sources", "built_in_database", "username"]),
            [?USERNAME_RULES_EXAMPLE]
        ),
    %% add rules for tenant_bar
    {ok, 204, _} =
        request_t(
            post,
            ?TENANT_BAR,
            uri(["authorization", "sources", "built_in_database", "username"]),
            [?USERNAME_RULES_EXAMPLE]
        ),
    %% return all rules if disable tenant header
    {ok, 200, RespAll} =
        request_t(
            get,
            ?DISABLE_TENANT_HEADER,
            uri(["authorization", "sources", "built_in_database", "username"])
        ),
    #{
        <<"data">> := [
            #{<<"username">> := <<"user1">>, <<"rules">> := _},
            #{<<"username">> := <<"user1">>, <<"rules">> := _}
        ]
    } = jsx:decode(RespAll),
    %% return tenant_foo rules if set header with tenant_foo
    {ok, 200, Resp1} =
        request_t(
            get,
            ?TENANT_FOO,
            uri(["authorization", "sources", "built_in_database", "username"])
        ),
    #{<<"data">> := [#{<<"username">> := <<"user1">>, <<"rules">> := _}]} = jsx:decode(Resp1),

    %% remove tenant_foo rules only
    {ok, 204, _} =
        request_t(
            delete,
            ?TENANT_FOO,
            uri(["authorization", "sources", "built_in_database", "username", "user1"])
        ),
    {ok, 200, Resp2} =
        request_t(
            get,
            ?TENANT_FOO,
            uri(["authorization", "sources", "built_in_database", "username"])
        ),
    #{<<"data">> := []} = jsx:decode(Resp2),
    {ok, 200, Resp3} =
        request_t(
            get,
            ?TENANT_BAR,
            uri(["authorization", "sources", "built_in_database", "username"])
        ),
    #{<<"data">> := [#{<<"username">> := <<"user1">>, <<"rules">> := _}]} = jsx:decode(Resp3),
    %% clean up
    {ok, 204, _} =
        request_t(
            delete,
            ?TENANT_BAR,
            uri(["authorization", "sources", "built_in_database", "username", "user1"])
        ).
