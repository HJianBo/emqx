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

-module(emqx_tenancy_sample_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TENANT_FOO, <<"tenant_foo">>).
-define(HOST, <<"tenant_foo.emqxserver.io">>).

%%--------------------------------------------------------------------
%% setup
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_dashboard_api_test_helpers:init_suite(
        [emqx_conf, emqx_authn, emqx_authz, emqx_retainer, emqx_tenancy],
        fun set_special_configs/1
    ),
    ok = create_tenant(?TENANT_FOO),
    ClientFn = emqx_tenant_test_helpers:reload_listener_with_ppv2(
        [listeners, tcp, default],
        ?HOST
    ),
    [{client_fn, ClientFn} | Config].

end_per_suite(_) ->
    %% revert to default value
    {ok, _} = emqx:update_config(
        [authorization],
        #{
            <<"no_match">> => <<"allow">>,
            <<"cache">> => #{<<"enable">> => <<"true">>},
            <<"sources">> => []
        }
    ),
    ok = delete_tenant(?TENANT_FOO),
    emqx_tenant_test_helpers:reload_listener_without_ppv2([listeners, tcp, default]),
    emqx_dashboard_api_test_helpers:end_suite([
        emqx_tenancy, emqx_retainer, emqx_authz, emqx_authn, emqx_conf
    ]).

set_special_configs(emqx) ->
    %% restart gen_rpc with `stateless` mode
    application:set_env(gen_rpc, port_discovery, stateless),
    ok = application:stop(gen_rpc),
    ok = application:start(gen_rpc);
set_special_configs(emqx_authz) ->
    {ok, _} = emqx:update_config([authorization, sources], []);
set_special_configs(emqx_tenancy) ->
    {ok, _} = emqx:update_config([tenant, sample_interval], 1000);
set_special_configs(_) ->
    ok.

%%--------------------------------------------------------------------
%% tests
%%--------------------------------------------------------------------

t_client_disconnected(Config) ->
    ClientFn = proplists:get_value(client_fn, Config),
    ClientId = <<"clientid">>,

    {ok, C} = emqtt:start_link([
        {clean_start, true},
        {host, "127.0.0.1"},
        {port, 8083},
        {clientid, <<"emqx_c">>}
    ]),
    {ok, _} = emqtt:ws_connect(C),
    {ok, undefined, [0]} = emqtt:subscribe(C, <<"$SYS/$TENANT/metrics">>, 0),
    {ok, Msg} = recv_msg(),
    ?assertMatch(
        #{
            client_pid := _,
            dup := false,
            qos := 0,
            retain := false,
            topic := <<"$SYS/$TENANT/metrics">>
        },
        Msg
    ),
    #{payload := Payload} = Msg,
    ?assertMatch(
        #{
            <<"timestamp">> := _,
            <<"node">> := _,
            <<"data">> := [
                #{
                    <<"tenant_id">> := ?TENANT_FOO,
                    <<"topics">> := 0,
                    <<"subscriptions_shared">> := 0,
                    <<"subscriptions">> := 0,
                    <<"sessions">> := 0,
                    <<"received_msg_rate">> := 0,
                    <<"msg_sent">> := 0,
                    <<"msg_retained">> := 0,
                    <<"byte_sent">> := 0,
                    <<"byte_received">> := 0,
                    <<"connections">> := 0
                }
            ]
        },
        emqx_json:decode(Payload, [return_maps])
    ),

    {ok, ClientPid} = ClientFn(ClientId, #{proto_ver => v5}),
    {ok, _, [0]} = emqtt:subscribe(ClientPid, <<"test">>, 0),

    ?assertMatch(
        {ok, #{sessions := #{max := 5, used := 1}}},
        emqx_tenancy_quota:info(?TENANT_FOO)
    ),

    {ok, _Msg1} = recv_msg(),
    {ok, Msg2} = recv_msg(),
    #{payload := Payload2} = Msg2,
    ?assertMatch(
        #{
            <<"timestamp">> := _,
            <<"node">> := _,
            <<"data">> := [
                #{
                    <<"tenant_id">> := ?TENANT_FOO,
                    <<"topics">> := 1,
                    <<"subscriptions_shared">> := 0,
                    <<"subscriptions">> := 1,
                    <<"sessions">> := 1,
                    <<"received_msg_rate">> := 0,
                    <<"msg_sent">> := 0,
                    <<"msg_retained">> := 0,
                    <<"byte_sent">> := 0,
                    <<"byte_received">> := 0,
                    <<"connections">> := 1
                }
            ]
        },
        emqx_json:decode(Payload2, [return_maps])
    ),

    emqtt:disconnect(ClientPid),
    emqtt:disconnect(C),
    timer:sleep(1000),
    ?assertMatch(
        {ok, #{sessions := #{max := 5, used := 0}}},
        emqx_tenancy_quota:info(?TENANT_FOO)
    ),
    ok.

t_tenant_deleted(Config) ->
    process_flag(trap_exit, true),
    ClientFn = proplists:get_value(client_fn, Config),
    ClientId = <<"clientid">>,
    {ok, ClientPid} = ClientFn(ClientId, #{proto_ver => v5}),
    ?assertMatch(
        {ok, #{sessions := #{max := 5, used := 1}}},
        emqx_tenancy_quota:info(?TENANT_FOO)
    ),

    ok = delete_tenant(?TENANT_FOO),
    timer:sleep(1000),
    ?assertMatch(false, erlang:is_process_alive(ClientPid)),
    {ok, C} = emqtt:start_link([
        {clean_start, true},
        {host, "127.0.0.1"},
        {port, 8083},
        {clientid, <<"emqx_c">>}
    ]),
    {ok, _} = emqtt:ws_connect(C),
    {ok, undefined, [0]} = emqtt:subscribe(C, <<"$SYS/$TENANT/metrics">>, 0),
    ?assertMatch({error, timeout}, recv_msg()),
    ok = create_tenant(?TENANT_FOO),
    ok.

%%--------------------------------------------------------------------
%% helper

create_tenant(Id) ->
    {ok, _} = emqx_tenancy:create(#{
        <<"id">> => Id,
        <<"configs">> => #{
            <<"quotas">> => #{
                <<"max_sessions">> => 5,
                <<"max_auhtn_users">> => 1,
                <<"max_authz_rules">> => 1
            }
        },
        <<"enabled">> => true,
        <<"desc">> => <<>>
    }),
    ok.

delete_tenant(Id) ->
    ok = emqx_tenancy:delete(Id).

recv_msg() ->
    receive
        {publish, Msg} ->
            {ok, Msg}
    after 2000 ->
        {error, timeout}
    end.
