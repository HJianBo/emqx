%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_mnesia_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/emqx.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
        {group, no_tenant},
        {group, tenant_foo}
    ].

groups() ->
    CTs = emqx_common_test_helpers:all(?MODULE),
    [
        {no_tenant, [], CTs},
        {tenant_foo, [], CTs}
    ].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_authz],
        fun set_special_configs/1
    ),
    Config.

end_per_suite(_Config) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    ok = emqx_common_test_helpers:stop_apps([emqx_authz]).

init_per_group(no_tenant, Config) ->
    [{tenant, ?NO_TENANT} | Config];
init_per_group(tenant_foo, Config) ->
    [{tenant, <<"tenant_foo">>} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = emqx_authz_test_lib:reset_authorizers(),
    ok = setup_config(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_authz_mnesia:purge_rules().

set_special_configs(emqx_authz) ->
    ok = emqx_authz_test_lib:reset_authorizers();
set_special_configs(_) ->
    ok.

%%--------------------------------------------------------------------
%% Testcases
%%--------------------------------------------------------------------
t_username_topic_rules(Config) ->
    Tenant = proplists:get_value(tenant, Config),
    ok = test_topic_rules(Tenant, username).

t_clientid_topic_rules(Config) ->
    Tenant = proplists:get_value(tenant, Config),
    ok = test_topic_rules(Tenant, clientid).

t_all_topic_rules(Config) ->
    Tenant = proplists:get_value(tenant, Config),
    ok = test_topic_rules(Tenant, all).

test_topic_rules(Tenant, Key) ->
    ClientInfo = #{
        tenant_id => Tenant,
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    SetupSamples = fun(CInfo, Samples) ->
        setup_client_samples(CInfo, Samples, Key)
    end,

    ok = emqx_authz_test_lib:test_no_topic_rules(ClientInfo, SetupSamples),

    ok = emqx_authz_test_lib:test_allow_topic_rules(ClientInfo, SetupSamples),

    ok = emqx_authz_test_lib:test_deny_topic_rules(ClientInfo, SetupSamples).

t_normalize_rules(Config) ->
    Tenant = proplists:get_value(tenant, Config),
    ClientInfo = #{
        tenant_id => Tenant,
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => {tcp, default}
    },

    ok = emqx_authz_mnesia:store_rules(
        {username, Tenant, <<"username">>},
        [{allow, publish, "t"}]
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, publish, <<"t">>)
    ),

    ?assertException(
        error,
        {invalid_rule, _},
        emqx_authz_mnesia:store_rules(
            {username, Tenant, <<"username">>},
            [[allow, publish, <<"t">>]]
        )
    ),

    ?assertException(
        error,
        {invalid_rule_action, _},
        emqx_authz_mnesia:store_rules(
            {username, Tenant, <<"username">>},
            [{allow, pub, <<"t">>}]
        )
    ),

    ?assertException(
        error,
        {invalid_rule_permission, _},
        emqx_authz_mnesia:store_rules(
            {username, Tenant, <<"username">>},
            [{accept, publish, <<"t">>}]
        )
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

raw_mnesia_authz_config() ->
    #{
        <<"enable">> => <<"true">>,
        <<"type">> => <<"built_in_database">>
    }.

setup_client_samples(ClientInfo, Samples, Key) ->
    Tenant = maps:get(tenant_id, ClientInfo),
    ok = emqx_authz_mnesia:purge_rules(),
    Rules = lists:flatmap(
        fun(#{topics := Topics, permission := Permission, action := Action}) ->
            lists:map(
                fun(Topic) ->
                    {binary_to_atom(Permission), binary_to_atom(Action), Topic}
                end,
                Topics
            )
        end,
        Samples
    ),
    #{username := Username, clientid := ClientId} = ClientInfo,
    Who =
        case Key of
            username -> {username, Tenant, Username};
            clientid -> {clientid, Tenant, ClientId};
            all -> {all, Tenant}
        end,
    ok = emqx_authz_mnesia:store_rules(Who, Rules).

setup_config() ->
    emqx_authz_test_lib:setup_config(raw_mnesia_authz_config(), #{}).
