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

-module(emqx_tenant_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CERTS_PATH(CertName),
    list_to_binary(
        filename:join(["../../lib/emqx/etc/certs/", CertName])
    )
).

-define(DEFAULT_CLIENTID, <<"client1">>).

-define(TENANT_FOO, <<"tenant_foo">>).
-define(TENANT_FOO_SNI, "tenant_foo.emqxserver.io").

-define(TENANT_BAR, <<"tenant_bar">>).
-define(TENANT_BAR_SNI, "tenant_bar.emqxserver.io").

%%--------------------------------------------------------------------
%% setups
%%--------------------------------------------------------------------

all() ->
    [
        %{group, tcp_ppv2},
        %{group, ws_ppv2},
        {group, ssl}
        %{group, wss}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [
        {tcp_ppv2, [], TCs},
        {ws_ppv2, [], TCs},
        {ssl, [], TCs},
        {wss, [], TCs}
    ].

init_per_suite(Config) ->
    %% start standalone emqx without emqx_machine and emqx_conf
    emqx_common_test_helpers:start_apps([]),
    %% load the testing confs
    emqx_common_test_helpers:load_config(emqx_schema, listener_confs()),
    emqx_common_test_helpers:load_config(emqx_schema, tenant_confs()),
    %% restart listeners
    emqx_listeners:restart(),
    Config.

end_per_suite(_) ->
    %% stop standalone emqx
    emqx_common_test_helpers:stop_apps([]),
    %% clear confs
    ok = emqx_config:erase(listeners),
    ok = emqx_config:erase(tenant),
    ok.

init_per_group(GroupName, Config) ->
    [
        {group, GroupName},
        {client_conn_fn, client_conn_fn(GroupName)}
        | Config
    ].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    meck_recv_ppv2(proplists:get_value(group, Config)),
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase(init, Config);
        _ -> Config
    end.

end_per_testcase(TestCase, Config) ->
    clear_meck_recv_ppv2(proplists:get_value(group, Config)),
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase('end', Config);
        false -> ok
    end,
    Config.

client_conn_fn(tcp_ppv2) ->
    client_conn_fn_gen(connect, #{ssl => false, port => 1883});
client_conn_fn(ws_ppv2) ->
    client_conn_fn_gen(ws_connect, #{ssl => false, port => 8083});
client_conn_fn(ssl) ->
    client_conn_fn_gen(connect, #{ssl => true, port => 8883});
client_conn_fn(wss) ->
    client_conn_fn_gen(ws_connect, #{ssl => true, port => 8084}).

client_conn_fn_gen(Connect, Opts0) ->
    fun(ClientId, Opts1) ->
        {ok, C} = emqtt:start_link(maps:merge(Opts0, Opts1#{clientid => ClientId})),
        case emqtt:Connect(C) of
            {ok, _} -> {ok, C};
            {error, _} = Err -> Err
        end
    end.

listener_confs() ->
    CaCert = ?CERTS_PATH("cacert.pem"),
    Cert = ?CERTS_PATH("cert.pem"),
    Key = ?CERTS_PATH("key.pem"),
    <<
        ""
        "\n"
        "listeners.tcp.default {\n"
        "  bind = \"0.0.0.0:1883\"\n"
        "  proxy_protocol = true\n"
        "  max_connections = 1024000\n"
        "}\n"
        "\n"
        "listeners.ssl.default {\n"
        "  bind = \"0.0.0.0:8883\"\n"
        "  max_connections = 512000\n"
        "  ssl_options {\n"
        "    keyfile = \"",
        Key/binary,
        "\"\n"
        "    certfile = \"",
        Cert/binary,
        "\"\n"
        "    cacertfile = \"",
        CaCert/binary,
        "\"\n"
        "  }\n"
        "}\n"
        "\n"
        "listeners.ws.default {\n"
        "  bind = \"0.0.0.0:8083\"\n"
        "  proxy_protocol = true\n"
        "  max_connections = 1024000\n"
        "  websocket.mqtt_path = \"/mqtt\"\n"
        "}\n"
        "\n"
        "listeners.wss.default {\n"
        "  bind = \"0.0.0.0:8084\"\n"
        "  max_connections = 512000\n"
        "  websocket.mqtt_path = \"/mqtt\"\n"
        "  ssl_options {\n"
        "    keyfile = \"",
        Key/binary,
        "\"\n"
        "    certfile = \"",
        Cert/binary,
        "\"\n"
        "    cacertfile = \"",
        CaCert/binary,
        "\"\n"
        "  }\n"
        "}"
        ""
    >>.

tenant_confs() ->
    <<
        ""
        "\n"
        "tenant {\n"
        "  tenant_id_from = peersni\n"
        "  allow_undefined_tenant_access = true\n"
        "  topic_prefix = \"$tenants/${tenant_id}/\"\n"
        "}\n"
        ""
    >>.

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_tenant_id_from_peersni(Config) ->
    ClientFn = proplists:get_value(client_conn_fn, Config),
    %% allow duplicated clientid access in different tenants
    {ok, C1} = ClientFn(?DEFAULT_CLIENTID, #{
        ssl_opts => [{server_name_indication, ?TENANT_FOO_SNI}]
    }),
    {ok, C2} = ClientFn(?DEFAULT_CLIENTID, #{
        ssl_opts => [{server_name_indication, ?TENANT_BAR_SNI}]
    }),
    {ok, C3} = ClientFn(?DEFAULT_CLIENTID, #{ssl_opts => [{server_name_indication, disable}]}),
    %% assert the sni parsing
    ?assertMatch(
        #{clientinfo := #{tenant_id := ?TENANT_FOO}},
        emqx_cm:get_chan_info(emqx_clientid:with_tenant(?TENANT_FOO, ?DEFAULT_CLIENTID))
    ),
    ?assertMatch(
        #{clientinfo := #{tenant_id := ?TENANT_BAR}},
        emqx_cm:get_chan_info(emqx_clientid:with_tenant(?TENANT_BAR, ?DEFAULT_CLIENTID))
    ),
    ?assertMatch(
        #{clientinfo := #{tenant_id := ?NO_TENANT}},
        emqx_cm:get_chan_info(emqx_clientid:with_tenant(?NO_TENANT, ?DEFAULT_CLIENTID))
    ),
    ok = emqtt:disconnect(C1),
    ok = emqtt:disconnect(C2),
    ok = emqtt:disconnect(C3).

%t_pubsub_isolation(_) ->
%    ok.
%
%t_will_mesages_isolation(_) ->
%    ok.
%
%t_shared_subs_works_well(_) ->
%    ok.
%
%t_undefined_tenant_is_superman(_) ->
%    ok.

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

meck_recv_ppv2(GroupName) when GroupName == tcp_ppv2; GroupName == ws_ppv2 ->
    ok;
meck_recv_ppv2(_) ->
    ok.

clear_meck_recv_ppv2(GroupName) when GroupName == tcp_ppv2; GroupName == ws_ppv2 ->
    ok;
clear_meck_recv_ppv2(_) ->
    ok.
