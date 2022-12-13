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

-include_lib("esockd/include/esockd.hrl").

-define(CERTS_PATH(CertName),
    list_to_binary(
        filename:join(["../../lib/emqx/etc/certs/", CertName])
    )
).

-define(DEFAULT_CLIENTID, <<"client1">>).
-define(DEFAULT_OPTS, #{host => "127.0.0.1", proto_ver => v5, connect_timeout => 5, ssl => false}).

-define(TENANT_FOO, <<"tenant_foo">>).
-define(TENANT_FOO_SNI, "tenant_foo.emqxserver.io").

-define(TENANT_BAR, <<"tenant_bar">>).
-define(TENANT_BAR_SNI, "tenant_bar.emqxserver.io").

%%--------------------------------------------------------------------
%% setups
%%--------------------------------------------------------------------

all() ->
    [
        {group, tcp_ppv2},
        {group, ws_ppv2},
        {group, ssl}
        %{group, wss} FIXME: SNI is not available from cowboy, so it is not supported for now.
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
        {client_conn_fn, client_conn_fn(GroupName)},
        {internal_client_conn_fn, client_conn_fn_gen(connect, ?DEFAULT_OPTS#{port => 11883})}
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
    client_conn_fn_gen(connect, ?DEFAULT_OPTS#{port => 1883});
client_conn_fn(ws_ppv2) ->
    client_conn_fn_gen(ws_connect, ?DEFAULT_OPTS#{port => 8083});
client_conn_fn(ssl) ->
    client_conn_fn_gen(connect, ?DEFAULT_OPTS#{ssl => true, port => 8883});
client_conn_fn(wss) ->
    client_conn_fn_gen(ws_connect, ?DEFAULT_OPTS#{ssl => true, port => 8084}).

client_conn_fn_gen(Connect, Opts0) ->
    fun(ClientId, Opts1) ->
        prepare_sni_for_meck(Opts1),
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
        "listeners.tcp.internal {\n"
        "  bind = \"0.0.0.0:11883\"\n"
        "  tenant.tenant_id_from = none\n"
        "  max_connections = 1024000\n"
        "}\n"
        "\n"
        "listeners.tcp.default {\n"
        "  bind = \"0.0.0.0:1883\"\n"
        "  tenant.tenant_id_from = peersni\n"
        "  proxy_protocol = true\n"
        "  max_connections = 1024000\n"
        "}\n"
        "\n"
        "listeners.ssl.default {\n"
        "  bind = \"0.0.0.0:8883\"\n"
        "  tenant.tenant_id_from = peersni\n"
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
        "  tenant.tenant_id_from = peersni\n"
        "  proxy_protocol = true\n"
        "  max_connections = 1024000\n"
        "  websocket.mqtt_path = \"/mqtt\"\n"
        "}\n"
        "\n"
        "listeners.wss.default {\n"
        "  bind = \"0.0.0.0:8084\"\n"
        "  tenant.tenant_id_from = peersni\n"
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

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_tenant_id_from_peersni(Config) ->
    ClientFn = proplists:get_value(client_conn_fn, Config),
    InterClientFn = proplists:get_value(internal_client_conn_fn, Config),
    %% allow duplicated clientid access in different tenants
    {ok, C1} = ClientFn(?DEFAULT_CLIENTID, #{
        ssl_opts => [{server_name_indication, ?TENANT_FOO_SNI}]
    }),
    {ok, C2} = ClientFn(?DEFAULT_CLIENTID, #{
        ssl_opts => [{server_name_indication, ?TENANT_BAR_SNI}]
    }),
    {ok, C3} = InterClientFn(?DEFAULT_CLIENTID, #{}),
    %% assert tenant_id is prefix of sni
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

t_pubsub_topic_prefix(Config) ->
    ClientFn = proplists:get_value(client_conn_fn, Config),
    InterClientFn = proplists:get_value(internal_client_conn_fn, Config),
    {ok, C1} = InterClientFn(?DEFAULT_CLIENTID, #{}),
    {ok, C2} = ClientFn(?DEFAULT_CLIENTID, #{
        ssl_opts => [{server_name_indication, ?TENANT_FOO_SNI}]
    }),

    Topic = <<"test/1">>,
    FullTopic = <<"$tenants/tenant_foo/", Topic/binary>>,

    %% undefined tenant don't have topic prefix
    {ok, _, _} = emqtt:subscribe(C1, Topic),
    ?assertMatch([_], emqx_broker:subscribers(Topic)),
    {ok, _, _} = emqtt:unsubscribe(C1, Topic),
    ?assertMatch([], emqx_broker:subscribers(Topic)),

    %% assert topic prefix for tenant_foo's client subscribing
    {ok, _, _} = emqtt:subscribe(C2, Topic),
    ?assertMatch([], emqx_broker:subscribers(Topic)),
    ?assertMatch([_], emqx_broker:subscribers(FullTopic)),
    {ok, _, _} = emqtt:unsubscribe(C2, Topic),
    ?assertMatch([], emqx_broker:subscribers(FullTopic)),

    %% assert topic prefix for tenant_foo's client publish
    %% (pub to "test/1", recevied by "$tenants/tenant_foo/test/1")
    {ok, _, _} = emqtt:subscribe(C1, FullTopic),
    {ok, _} = emqtt:publish(C2, Topic, <<"msg from tenant_foo">>, qos1),
    ?assertMatch([#{client_pid := C1, topic := FullTopic}], acc_emqtt_publish(1)),

    ok = emqtt:disconnect(C1),
    ok = emqtt:disconnect(C2).

t_pubsub_isolation(Config) ->
    ClientFn = proplists:get_value(client_conn_fn, Config),
    ClientId1 = ?DEFAULT_CLIENTID,
    ClientId2 = <<"clientid2">>,
    ClientId3 = <<"clientid3">>,
    {ok, C1} = ClientFn(ClientId1, #{
        ssl_opts => [{server_name_indication, ?TENANT_FOO_SNI}]
    }),
    {ok, C2} = ClientFn(ClientId2, #{
        ssl_opts => [{server_name_indication, ?TENANT_FOO_SNI}]
    }),
    {ok, C3} = ClientFn(ClientId3, #{
        ssl_opts => [{server_name_indication, ?TENANT_BAR_SNI}]
    }),

    Topic = <<"test/1">>,
    {ok, _, _} = emqtt:subscribe(C1, Topic, qos1),
    %% should be recevied msg from client2 in same tenant
    {ok, _} = emqtt:publish(C2, Topic, <<"msg from client2">>, qos1),
    ?assertMatch([#{client_pid := C1, topic := Topic}], acc_emqtt_publish(1)),
    %% should not be recevied msg from client3 in different tenant
    {ok, _} = emqtt:publish(C3, Topic, <<"msg from client3">>, qos2),
    ?assertMatch([], acc_emqtt_publish(1, 2000)),

    ok = emqtt:disconnect(C1),
    ok = emqtt:disconnect(C2),
    ok = emqtt:disconnect(C3).

t_will_mesages_isolation(Config) ->
    process_flag(trap_exit, true),
    ClientFn = proplists:get_value(client_conn_fn, Config),
    InterClientFn = proplists:get_value(internal_client_conn_fn, Config),
    WillTopic = <<"will/t">>,
    FullWillTopic = <<"$tenants/tenant_foo/", WillTopic/binary>>,
    {ok, C1} = InterClientFn(?DEFAULT_CLIENTID, #{}),
    {ok, _C2} = ClientFn(?DEFAULT_CLIENTID, #{
        will_topic => WillTopic,
        will_payload => <<"from tenant_foo client1">>,
        will_qos => 1,
        ssl_opts => [{server_name_indication, ?TENANT_FOO_SNI}]
    }),

    {ok, _, _} = emqtt:subscribe(C1, FullWillTopic, qos1),
    %% assert the will message with topic prefix
    ok = emqx_cm:kick_session(emqx_clientid:with_tenant(?TENANT_FOO, ?DEFAULT_CLIENTID)),
    ?assertMatch([#{client_pid := C1, topic := FullWillTopic}], acc_emqtt_publish(1)),

    ok = emqtt:disconnect(C1).

t_shared_subs_works_well(Config) ->
    ClientFn = proplists:get_value(client_conn_fn, Config),
    ClientId1 = ?DEFAULT_CLIENTID,
    ClientId2 = <<"clientid2">>,
    ClientId3 = <<"clientid3">>,
    {ok, C1} = ClientFn(ClientId1, #{
        ssl_opts => [{server_name_indication, ?TENANT_FOO_SNI}]
    }),
    {ok, C2} = ClientFn(ClientId2, #{
        ssl_opts => [{server_name_indication, ?TENANT_FOO_SNI}]
    }),
    {ok, C3} = ClientFn(ClientId3, #{
        ssl_opts => [{server_name_indication, ?TENANT_FOO_SNI}]
    }),

    Topic = <<"test/1">>,
    FullTopic = <<"$tenants/tenant_foo/", Topic/binary>>,

    %% assert shared topic subscribed
    {ok, _, _} = emqtt:subscribe(C1, <<"$share/g1/", Topic/binary>>, qos1),
    {ok, _, _} = emqtt:subscribe(C2, <<"$share/g1/", Topic/binary>>, qos1),
    {ok, _, _} = emqtt:subscribe(C3, <<"$share/g1/", Topic/binary>>, qos1),
    ?assertMatch(
        [_, _, _],
        ets:select(
            emqx_shared_subscription,
            [{{emqx_shared_subscription, <<"g1">>, FullTopic, '$1'}, [], ['$1']}]
        )
    ),
    %% only one subscriber can received shared msg
    Msg = emqx_message:make(test, 1, FullTopic, <<"shared msgs">>),
    emqx_broker:publish(Msg),
    ?assertMatch([#{topic := Topic}], acc_emqtt_publish(3)),
    ok.

t_not_allowed_undefined_tenant(Config) ->
    process_flag(trap_exit, true),
    ClientFn = proplists:get_value(client_conn_fn, Config),
    {error, {banned, _}} = ClientFn(?DEFAULT_CLIENTID, #{
        ssl_opts => [{server_name_indication, disable}]
    }),
    %% allow tenant client
    {ok, C2} = ClientFn(?DEFAULT_CLIENTID, #{
        ssl_opts => [{server_name_indication, ?TENANT_FOO_SNI}]
    }),
    ok = emqtt:disconnect(C2),
    ok.

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

prepare_sni_for_meck(Opts) ->
    persistent_term:put(current_client_sni, sni_from_opts(Opts)).

sni_from_opts(Opts) ->
    SslOpts = maps:get(ssl_opts, Opts, []),
    case proplists:get_value(server_name_indication, SslOpts) of
        SNI when is_list(SNI) -> list_to_binary(SNI);
        _ -> undefined
    end.

meck_recv_ppv2(tcp_ppv2) ->
    ok = meck:new(esockd_proxy_protocol, [passthrough, no_history]),
    ok = meck:expect(
        esockd_proxy_protocol,
        recv,
        fun(_Transport, Socket, _Timeout) ->
            SNI = persistent_term:get(current_client_sni, undefined),
            {ok, {SrcAddr, SrcPort}} = esockd_transport:peername(Socket),
            {ok, {DstAddr, DstPort}} = esockd_transport:sockname(Socket),
            {ok, #proxy_socket{
                inet = inet4,
                socket = Socket,
                src_addr = SrcAddr,
                dst_addr = DstAddr,
                src_port = SrcPort,
                dst_port = DstPort,
                pp2_additional_info = [{pp2_authority, SNI}]
            }}
        end
    );
meck_recv_ppv2(ws_ppv2) ->
    ok = meck:new(ranch_tcp, [passthrough, no_history]),
    ok = meck:expect(
        ranch_tcp,
        recv_proxy_header,
        fun(Socket, _Timeout) ->
            SNI = persistent_term:get(current_client_sni, undefined),
            {ok, {SrcAddr, SrcPort}} = esockd_transport:peername(Socket),
            {ok, {DstAddr, DstPort}} = esockd_transport:sockname(Socket),
            {ok, #{
                authority => SNI,
                command => proxy,
                dest_address => DstAddr,
                dest_port => DstPort,
                src_address => SrcAddr,
                src_port => SrcPort,
                transport_family => ipv4,
                transport_protocol => stream,
                version => 2
            }}
        end
    );
meck_recv_ppv2(_) ->
    ok.

clear_meck_recv_ppv2(tcp_ppv2) ->
    ok = meck:unload(esockd_proxy_protocol);
clear_meck_recv_ppv2(ws_ppv2) ->
    ok = meck:unload(ranch_tcp);
clear_meck_recv_ppv2(_) ->
    ok.

acc_emqtt_publish(N) ->
    acc_emqtt_publish(N, 5000, []).

acc_emqtt_publish(N, Timeout) ->
    acc_emqtt_publish(N, Timeout, []).

acc_emqtt_publish(0, _Timeout, Acc) ->
    lists:reverse(Acc);
acc_emqtt_publish(N, Timeout, Acc) ->
    receive
        {publish, Msg} ->
            acc_emqtt_publish(N - 1, Timeout, [Msg | Acc])
    after Timeout ->
        lists:reverse(Acc)
    end.
