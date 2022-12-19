%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_tenant_api_publish_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(CLIENTID, <<"api_clientid">>).
-define(USERNAME, <<"api_username">>).

-define(TOPIC1, <<"api_topic1">>).
-define(TOPIC2, <<"api_topic2">>).
-define(HOST, <<"tenant_foo.emqxserver.io">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite(),
    ClientFn = emqx_tenant_test_helpers:reload_listener_with_ppv2(
        [listeners, tcp, default],
        ?HOST
    ),
    [{client_fn, ClientFn} | Config].

end_per_suite(_) ->
    emqx_tenant_test_helpers:reload_listener_without_ppv2([listeners, tcp, default]),
    emqx_mgmt_api_test_util:end_suite().

init_per_testcase(Case, Config) ->
    ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
    ?MODULE:Case({'end', Config}).

t_publish_api({init, Config}) ->
    Config;
t_publish_api({'end', _Config}) ->
    ok;
t_publish_api(Config) ->
    ClientFn = proplists:get_value(client_fn, Config),
    {ok, Client} = ClientFn(<<"api_clientid">>, #{username => <<"api_username">>}),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC1),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC2),
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish"]),
    Body = #{topic => ?TOPIC1, payload => Payload},
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    %% without tenant_id header
    {ok, ResponseError} = emqx_mgmt_api_test_util:request_api(post, Path, "", Auth, Body),
    ResponseErrorMap = decode_json(ResponseError),
    ?assertEqual(
        [<<"message">>, <<"reason_code">>],
        lists:sort(maps:keys(ResponseErrorMap)),
        ResponseErrorMap
    ),
    ?assertMatch(#{<<"reason_code">> := ?RC_NO_MATCHING_SUBSCRIBERS}, ResponseErrorMap),

    %% without bad tenant_id header
    BadTenantHeaders = [Auth, emqx_tenant_test_helpers:tenant_header(<<"test.t">>)],
    {ok, ResponseError1} = emqx_mgmt_api_test_util:request_api(
        post, Path, "", BadTenantHeaders, Body
    ),
    ResponseErrorMap1 = decode_json(ResponseError1),
    ?assertEqual(
        [<<"message">>, <<"reason_code">>],
        lists:sort(maps:keys(ResponseErrorMap1)),
        ResponseErrorMap1
    ),
    ?assertMatch(#{<<"reason_code">> := ?RC_NO_MATCHING_SUBSCRIBERS}, ResponseErrorMap1),

    %% with tenant_id header
    TenantHeaders = tenant_headers(Auth),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(post, Path, "", TenantHeaders, Body),
    ResponseMap = decode_json(Response),
    ?assertEqual([<<"id">>], lists:sort(maps:keys(ResponseMap)), ResponseMap),
    ?assertEqual(ok, receive_assert(?TOPIC1, 0, Payload)),
    emqtt:stop(Client).

t_publish_no_subscriber({init, Config}) ->
    Config;
t_publish_no_subscriber({'end', _Config}) ->
    ok;
t_publish_no_subscriber(_) ->
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish"]),
    TenantHeaders = tenant_headers(),
    Body = #{topic => ?TOPIC1, payload => Payload},
    {ok, Response} = emqx_mgmt_api_test_util:request_api(post, Path, "", TenantHeaders, Body),
    ResponseMap = decode_json(Response),
    ?assertEqual([<<"message">>, <<"reason_code">>], lists:sort(maps:keys(ResponseMap))),
    ?assertMatch(#{<<"reason_code">> := ?RC_NO_MATCHING_SUBSCRIBERS}, ResponseMap),
    ok.

t_publish_bad_topic({init, Config}) ->
    Config;
t_publish_bad_topic({'end', _Config}) ->
    ok;
t_publish_bad_topic(_) ->
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish"]),
    TenantHeaders = tenant_headers(),
    Body = #{topic => <<"not/a+/valid/topic">>, payload => Payload},
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(post, Path, "", TenantHeaders, Body)
    ).

t_publish_bad_base64({init, Config}) ->
    Config;
t_publish_bad_base64({'end', _Config}) ->
    ok;
t_publish_bad_base64(_) ->
    %% not a valid base64
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish"]),
    TenantHeaders = tenant_headers(),
    Body = #{
        topic => <<"not/a+/valid/topic">>, payload => Payload, payload_encoding => <<"base64">>
    },
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(post, Path, "", TenantHeaders, Body)
    ).

t_publish_too_large({init, Config}) ->
    MaxPacketSize = 100,
    meck:new(emqx_config, [no_link, passthrough, no_history]),
    meck:expect(emqx_config, get, fun
        ([mqtt, max_packet_size]) ->
            MaxPacketSize;
        (Other) ->
            meck:passthrough(Other)
    end),
    [{max_packet_size, MaxPacketSize} | Config];
t_publish_too_large({'end', _Config}) ->
    meck:unload(emqx_config),
    ok;
t_publish_too_large(Config) ->
    MaxPacketSize = proplists:get_value(max_packet_size, Config),
    Payload = lists:duplicate(MaxPacketSize, $0),
    Path = emqx_mgmt_api_test_util:api_path(["publish"]),
    TenantHeaders = tenant_headers(),
    Body = #{topic => <<"random/topic">>, payload => Payload},
    {error, {Summary, _Headers, ResponseBody}} =
        emqx_mgmt_api_test_util:request_api(
            post,
            Path,
            "",
            TenantHeaders,
            Body,
            #{return_all => true}
        ),
    ?assertMatch({_, 400, _}, Summary),
    ?assertMatch(
        #{
            <<"reason_code">> := ?RC_QUOTA_EXCEEDED,
            <<"message">> := <<"packet_too_large">>
        },
        decode_json(ResponseBody)
    ),
    ok.

t_publish_bad_topic_bulk({init, Config}) ->
    Config;
t_publish_bad_topic_bulk({'end', _Config}) ->
    ok;
t_publish_bad_topic_bulk(_Config) ->
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish", "bulk"]),
    TenantHeaders = tenant_headers(),
    Body = [
        #{topic => <<"not/a+/valid/topic">>, payload => Payload},
        #{topic => <<"good/topic">>, payload => Payload}
    ],
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(post, Path, "", TenantHeaders, Body)
    ).

t_publish_bulk_api({init, Config}) ->
    Config;
t_publish_bulk_api({'end', _Config}) ->
    ok;
t_publish_bulk_api(Config) ->
    ClientFn = proplists:get_value(client_fn, Config),
    {ok, Client} = ClientFn(<<"api_clientid">>, #{username => <<"api_username">>}),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC1),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC2),
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish", "bulk"]),
    TenantHeaders = tenant_headers(),
    Body = [
        #{
            topic => ?TOPIC1,
            payload => Payload,
            payload_encoding => plain
        },
        #{
            topic => ?TOPIC2,
            payload => base64:encode(Payload),
            payload_encoding => base64
        }
    ],
    {ok, Response} = emqx_mgmt_api_test_util:request_api(post, Path, "", TenantHeaders, Body),
    ResponseList = decode_json(Response),
    ?assertEqual(2, erlang:length(ResponseList)),
    lists:foreach(
        fun(ResponseMap) ->
            ?assertMatch(
                [<<"id">>], lists:sort(maps:keys(ResponseMap))
            )
        end,
        ResponseList
    ),
    ?assertEqual(ok, receive_assert(?TOPIC1, 0, Payload)),
    ?assertEqual(ok, receive_assert(?TOPIC2, 0, Payload)),
    emqtt:stop(Client).

t_publish_no_subscriber_bulk({init, Config}) ->
    Config;
t_publish_no_subscriber_bulk({'end', _Config}) ->
    ok;
t_publish_no_subscriber_bulk(Config) ->
    ClientFn = proplists:get_value(client_fn, Config),
    {ok, Client} = ClientFn(<<"api_clientid">>, #{username => <<"api_username">>}),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC1),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC2),
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish", "bulk"]),
    TenantHeaders = tenant_headers(),
    Body = [
        #{topic => ?TOPIC1, payload => Payload},
        #{topic => ?TOPIC2, payload => Payload},
        #{topic => <<"no/subscrbier/topic">>, payload => Payload}
    ],
    {ok, Response} = emqx_mgmt_api_test_util:request_api(post, Path, "", TenantHeaders, Body),
    ResponseList = decode_json(Response),
    ?assertMatch(
        [
            #{<<"id">> := _},
            #{<<"id">> := _},
            #{<<"message">> := <<"no_matching_subscribers">>}
        ],
        ResponseList
    ),
    ?assertEqual(ok, receive_assert(?TOPIC1, 0, Payload)),
    ?assertEqual(ok, receive_assert(?TOPIC2, 0, Payload)),
    emqtt:stop(Client).

t_publish_bulk_dispatch_one_message_invalid_topic({init, Config}) ->
    Config;
t_publish_bulk_dispatch_one_message_invalid_topic({'end', _Config}) ->
    ok;
t_publish_bulk_dispatch_one_message_invalid_topic(Config) when is_list(Config) ->
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish", "bulk"]),
    TenantHeaders = tenant_headers(),
    Body = [
        #{topic => ?TOPIC1, payload => Payload},
        #{topic => ?TOPIC2, payload => Payload},
        #{topic => <<"bad/#/topic">>, payload => Payload}
    ],
    {error, {Summary, _Headers, ResponseBody}} =
        emqx_mgmt_api_test_util:request_api(
            post,
            Path,
            "",
            TenantHeaders,
            Body,
            #{return_all => true}
        ),
    ?assertMatch({_, 400, _}, Summary),
    ?assertMatch(
        #{<<"reason_code">> := ?RC_TOPIC_NAME_INVALID},
        decode_json(ResponseBody)
    ).

t_publish_bulk_dispatch_failure({init, Config}) ->
    meck:new(emqx, [no_link, passthrough, no_history]),
    meck:expect(emqx, is_running, fun() -> false end),
    Config;
t_publish_bulk_dispatch_failure({'end', _Config}) ->
    meck:unload(emqx),
    ok;
t_publish_bulk_dispatch_failure(Config) when is_list(Config) ->
    ClientFn = proplists:get_value(client_fn, Config),
    {ok, Client} = ClientFn(<<"api_clientid">>, #{username => <<"api_username">>}),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC1),
    {ok, _, [0]} = emqtt:subscribe(Client, ?TOPIC2),
    Payload = <<"hello">>,
    Path = emqx_mgmt_api_test_util:api_path(["publish", "bulk"]),
    TenantHeaders = tenant_headers(),
    Body = [
        #{topic => ?TOPIC1, payload => Payload},
        #{topic => ?TOPIC2, payload => Payload},
        #{topic => <<"no/subscrbier/topic">>, payload => Payload}
    ],
    {error, {Summary, _Headers, ResponseBody}} =
        emqx_mgmt_api_test_util:request_api(
            post,
            Path,
            "",
            TenantHeaders,
            Body,
            #{return_all => true}
        ),
    ?assertMatch({_, 503, _}, Summary),
    ?assertMatch(
        [
            #{<<"reason_code">> := ?RC_IMPLEMENTATION_SPECIFIC_ERROR},
            #{<<"reason_code">> := ?RC_IMPLEMENTATION_SPECIFIC_ERROR},
            #{<<"reason_code">> := ?RC_NO_MATCHING_SUBSCRIBERS}
        ],
        decode_json(ResponseBody)
    ),
    emqtt:stop(Client).

receive_assert(Topic, Qos, Payload) ->
    receive
        {publish, Message} ->
            ReceiveTopic = maps:get(topic, Message),
            ReceiveQos = maps:get(qos, Message),
            ReceivePayload = maps:get(payload, Message),
            ?assertEqual(ReceiveTopic, Topic),
            ?assertEqual(ReceiveQos, Qos),
            ?assertEqual(ReceivePayload, Payload),
            ok
    after 5000 ->
        timeout
    end.

decode_json(In) ->
    emqx_json:decode(In, [return_maps]).

tenant_headers() ->
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    tenant_headers(Auth).

tenant_headers(Auth) ->
    [Auth, emqx_tenant_test_helpers:tenant_header(?HOST)].
