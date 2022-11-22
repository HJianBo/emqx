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

-module(emqx_authn_api_mnesia_tenancy_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(DISABLE_TENANT_HEADER, disable).
-define(TENANT_BAR, "tenant_bar").
-define(TENANT_FOO, "tenant_foo").

-import(emqx_dashboard_api_test_helpers, [uri/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_authn, emqx_dashboard],
        fun set_special_configs/1
    ),
    %% start built-in database authn
    {ok, 200, _} =
        request(
            post,
            ?DISABLE_TENANT_HEADER,
            uri(["authentication"]),
            #{
                <<"backend">> => <<"built_in_database">>,
                <<"mechanism">> => <<"password_based">>,
                <<"password_hash_algorithm">> => #{
                    <<"name">> => <<"sha256">>,
                    <<"salt_position">> => <<"suffix">>
                },
                <<"user_id_type">> => <<"username">>
            }
        ),
    Config.

end_per_suite(_Config) ->
    {ok, 204, _} =
        request(
            delete,
            ?DISABLE_TENANT_HEADER,
            uri(["authentication", "password_based:built_in_database"])
        ),
    emqx_common_test_helpers:stop_apps([emqx_dashboard, emqx_authn, emqx_conf]),
    ok.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config();
set_special_configs(emqx_authn) ->
    ok;
set_special_configs(_App) ->
    ok.

%%--------------------------------------------------------------------
%% tests
%%--------------------------------------------------------------------

t_isolation(_) ->
    %% add user for tenant_foo
    {ok, 201, _} =
        request(
            post,
            ?TENANT_FOO,
            uri(["authentication", "password_based:built_in_database", "users"]),
            #{
                <<"user_id">> => <<"user1">>,
                <<"password">> => <<"pwd2">>,
                <<"is_superuser">> => false
            }
        ),
    %% add user for tenant_bar
    {ok, 201, _} =
        request(
            post,
            ?TENANT_BAR,
            uri(["authentication", "password_based:built_in_database", "users"]),
            #{
                <<"user_id">> => <<"user2">>,
                <<"password">> => <<"pwd2">>,
                <<"is_superuser">> => false
            }
        ),
    %% return all user if disable tenant header
    {ok, 200, RespAll} =
        request(
            get,
            ?DISABLE_TENANT_HEADER,
            uri(["authentication", "password_based:built_in_database", "users"])
        ),
    #{
        <<"data">> := [
            #{<<"user_id">> := _},
            #{<<"user_id">> := _}
        ]
    } = jsx:decode(RespAll),
    %% return tenant_foo rules if set header with tenant_foo
    {ok, 200, Resp1} =
        request(
            get,
            ?TENANT_FOO,
            uri(["authentication", "password_based:built_in_database", "users"])
        ),
    #{<<"data">> := [#{<<"user_id">> := <<"user1">>}]} = jsx:decode(Resp1),
    %% remove tenant_foo rules only
    {ok, 204, _} =
        request(
            delete,
            ?TENANT_FOO,
            uri(["authentication", "password_based:built_in_database", "users", "user1"])
        ),
    {ok, 200, Resp2} =
        request(
            get,
            ?TENANT_FOO,
            uri(["authentication", "password_based:built_in_database", "users"])
        ),
    #{<<"data">> := []} = jsx:decode(Resp2),
    {ok, 200, Resp3} =
        request(
            get,
            ?TENANT_BAR,
            uri(["authentication", "password_based:built_in_database", "users"])
        ),
    #{<<"data">> := [#{<<"user_id">> := <<"user2">>}]} = jsx:decode(Resp3),
    %% clean up
    {ok, 204, _} =
        request(
            delete,
            ?TENANT_BAR,
            uri(["authentication", "password_based:built_in_database", "users", "user2"])
        ).

%%--------------------------------------------------------------------
%% helpers

request(Method, Tenant, Url) when Method == get; Method == delete ->
    emqx_dashboard_api_test_helpers:request(
        <<"admin">>, Method, Url, [], headers(Tenant)
    ).

request(Method, Tenant, Url, Body) when Method == post; Method == put ->
    emqx_dashboard_api_test_helpers:request(
        <<"admin">>, Method, Url, Body, headers(Tenant)
    ).

headers(disable) ->
    [];
headers(Tenant) ->
    [{"EMQX-TENANT-ID", Tenant}].
