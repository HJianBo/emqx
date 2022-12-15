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
-module(emqx_mgmt_tenant_api_clients_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(TENANT, <<"tenant_foo">>).
-define(HOST, <<?TENANT/binary, <<".emqxserver.io">>/binary>>).

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

t_clients(Config) ->
    process_flag(trap_exit, true),
    ClientFn = proplists:get_value(client_fn, Config),

    Username1 = <<"user1">>,
    ClientId1 = <<"client1">>,

    Username2 = <<"user2">>,
    ClientId2 = <<"client2">>,

    Topic = <<"topic_1">>,
    Qos = 0,

    {ok, _} = ClientFn(ClientId1, #{username => Username1, proto_ver => v5}),
    {ok, _} = ClientFn(ClientId2, #{username => Username2}),

    timer:sleep(300),

    %% get /clients
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    ClientsPath = emqx_mgmt_api_test_util:api_path(["clients"]),
    {ok, Clients} = emqx_mgmt_api_test_util:request_api(get, ClientsPath, Auth),
    assert_clients(Clients, 2),

    GoodHeaders = [Auth, emqx_tenant_test_helpers:tenant_header(?HOST)],
    {ok, GoodClients} = emqx_mgmt_api_test_util:request_api(get, ClientsPath, GoodHeaders),
    assert_clients(GoodClients, 2),

    BadHeaders = [Auth, emqx_tenant_test_helpers:tenant_header(<<"bad.emqx.com">>)],
    {ok, BadClients} = emqx_mgmt_api_test_util:request_api(get, ClientsPath, BadHeaders),
    assert_clients(BadClients, 0),

    %% get /clients/:clientid
    Client1Path = emqx_mgmt_api_test_util:api_path(["clients", binary_to_list(ClientId1)]),
    ?assertEqual(
        {error, {"HTTP/1.1", 404, "Not Found"}},
        emqx_mgmt_api_test_util:request_api(get, Client1Path, Auth)
    ),
    {ok, Client1} = emqx_mgmt_api_test_util:request_api(get, Client1Path, GoodHeaders),
    Client1Response = emqx_json:decode(Client1, [return_maps]),
    ?assertEqual(Username1, maps:get(<<"username">>, Client1Response)),
    ?assertEqual(ClientId1, maps:get(<<"clientid">>, Client1Response)),
    ?assertEqual(
        {error, {"HTTP/1.1", 404, "Not Found"}},
        emqx_mgmt_api_test_util:request_api(get, Client1Path, BadHeaders)
    ),

    %% delete /clients/:clientid kick out
    Client2Path = emqx_mgmt_api_test_util:api_path(["clients", binary_to_list(ClientId2)]),
    ?assertEqual(
        {error, {"HTTP/1.1", 404, "Not Found"}},
        emqx_mgmt_api_test_util:request_api(delete, Client2Path, BadHeaders)
    ),
    {ok, _} = emqx_mgmt_api_test_util:request_api(get, Client2Path, GoodHeaders),
    {ok, _} = emqx_mgmt_api_test_util:request_api(delete, Client2Path, GoodHeaders),
    timer:sleep(300),
    AfterKickoutResponse2 = emqx_mgmt_api_test_util:request_api(get, Client2Path, GoodHeaders),
    ?assertEqual({error, {"HTTP/1.1", 404, "Not Found"}}, AfterKickoutResponse2),

    %% get /clients/:clientid/authorization/cache should has no authz cache
    Client1AuthzCachePath = emqx_mgmt_api_test_util:api_path([
        "clients",
        binary_to_list(ClientId1),
        "authorization",
        "cache"
    ]),
    {ok, Client1AuthzCache} = emqx_mgmt_api_test_util:request_api(
        get, Client1AuthzCachePath, GoodHeaders
    ),
    ?assertEqual("[]", Client1AuthzCache),

    %% post /clients/:clientid/subscribe
    SubscribeBody = #{topic => Topic, qos => Qos, nl => 1, rh => 1},
    SubscribePath = emqx_mgmt_api_test_util:api_path([
        "clients",
        binary_to_list(ClientId1),
        "subscribe"
    ]),
    {ok, _} = emqx_mgmt_api_test_util:request_api(
        post,
        SubscribePath,
        "",
        GoodHeaders,
        SubscribeBody
    ),
    timer:sleep(100),
    [{AfterSubTopic, #{qos := AfterSubQos}}] = emqx_mgmt:lookup_subscriptions({?TENANT, ClientId1}),
    ?assertMatch({_, _}, binary:match(AfterSubTopic, [Topic])),
    ?assertEqual(AfterSubQos, Qos),

    %% get /clients/:clientid/subscriptions
    SubscriptionsPath = emqx_mgmt_api_test_util:api_path([
        "clients",
        binary_to_list(ClientId1),
        "subscriptions"
    ]),
    {ok, SubscriptionsRes} = emqx_mgmt_api_test_util:request_api(
        get,
        SubscriptionsPath,
        "",
        GoodHeaders
    ),
    [SubscriptionsData] = emqx_json:decode(SubscriptionsRes, [return_maps]),
    ?assertMatch(
        #{
            <<"clientid">> := ClientId1,
            <<"nl">> := 1,
            <<"rap">> := 0,
            <<"rh">> := 1,
            <<"node">> := _,
            <<"qos">> := Qos,
            <<"topic">> := Topic
        },
        SubscriptionsData
    ),

    %% post /clients/:clientid/unsubscribe
    UnSubscribePath = emqx_mgmt_api_test_util:api_path([
        "clients",
        binary_to_list(ClientId1),
        "unsubscribe"
    ]),
    UnSubscribeBody = #{topic => Topic},
    {ok, _} = emqx_mgmt_api_test_util:request_api(
        post,
        UnSubscribePath,
        "",
        GoodHeaders,
        UnSubscribeBody
    ),
    timer:sleep(100),
    ?assertEqual([], emqx_mgmt:lookup_subscriptions(Client1)),

    %% testcase cleanup, kick out client1
    {ok, _} = emqx_mgmt_api_test_util:request_api(delete, Client1Path, GoodHeaders),
    timer:sleep(300),
    AfterKickoutResponse1 = emqx_mgmt_api_test_util:request_api(get, Client1Path, GoodHeaders),
    ?assertEqual({error, {"HTTP/1.1", 404, "Not Found"}}, AfterKickoutResponse1).

assert_clients(Clients, Count) ->
    ClientsResponse = emqx_json:decode(Clients, [return_maps]),
    ClientsMeta = maps:get(<<"meta">>, ClientsResponse),
    ClientsPage = maps:get(<<"page">>, ClientsMeta),
    ClientsLimit = maps:get(<<"limit">>, ClientsMeta),
    ClientsCount = maps:get(<<"count">>, ClientsMeta),
    ?assertEqual(ClientsPage, 1),
    ?assertEqual(ClientsLimit, emqx_mgmt:max_row_limit()),
    ?assertEqual(ClientsCount, Count, Clients).

t_query_clients_with_time(Config) ->
    process_flag(trap_exit, true),
    ClientFn = proplists:get_value(client_fn, Config),
    Username1 = <<"user1">>,
    ClientId1 = <<"client1">>,

    Username2 = <<"user2">>,
    ClientId2 = <<"client2">>,

    {ok, _} = ClientFn(ClientId1, #{username => Username1}),
    {ok, _} = ClientFn(ClientId2, #{username => Username2}),

    timer:sleep(100),

    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    GoodHeaders = [AuthHeader, emqx_tenant_test_helpers:tenant_header(?HOST)],
    ClientsPath = emqx_mgmt_api_test_util:api_path(["clients"]),
    %% get /clients with time(rfc3339)
    NowTimeStampInt = erlang:system_time(millisecond),
    %% Do not uri_encode `=` to `%3D`
    Rfc3339String = emqx_http_lib:uri_encode(
        binary:bin_to_list(
            emqx_datetime:epoch_to_rfc3339(NowTimeStampInt)
        )
    ),
    TimeStampString = emqx_http_lib:uri_encode(integer_to_list(NowTimeStampInt)),

    LteKeys = ["lte_created_at=", "lte_connected_at="],
    GteKeys = ["gte_created_at=", "gte_connected_at="],
    LteParamRfc3339 = [Param ++ Rfc3339String || Param <- LteKeys],
    LteParamStamp = [Param ++ TimeStampString || Param <- LteKeys],
    GteParamRfc3339 = [Param ++ Rfc3339String || Param <- GteKeys],
    GteParamStamp = [Param ++ TimeStampString || Param <- GteKeys],

    RequestResults =
        [
            emqx_mgmt_api_test_util:request_api(get, ClientsPath, Param, GoodHeaders)
         || Param <-
                LteParamRfc3339 ++ LteParamStamp ++
                    GteParamRfc3339 ++ GteParamStamp
        ],
    DecodedResults = [
        emqx_json:decode(Response, [return_maps])
     || {ok, Response} <- RequestResults
    ],
    {LteResponseDecodeds, GteResponseDecodeds} = lists:split(4, DecodedResults),
    %% EachData :: list()
    [
        ?assert(time_string_to_epoch_millisecond(CreatedAt) < NowTimeStampInt)
     || #{<<"data">> := EachData} <- LteResponseDecodeds,
        #{<<"created_at">> := CreatedAt} <- EachData
    ],
    [
        ?assert(time_string_to_epoch_millisecond(ConnectedAt) < NowTimeStampInt)
     || #{<<"data">> := EachData} <- LteResponseDecodeds,
        #{<<"connected_at">> := ConnectedAt} <- EachData
    ],
    [
        ?assertEqual(EachData, [])
     || #{<<"data">> := EachData} <- GteResponseDecodeds
    ],

    %% testcase cleanup, kickout client1 and client2
    Client1Path = emqx_mgmt_api_test_util:api_path(["clients", binary_to_list(ClientId1)]),
    Client2Path = emqx_mgmt_api_test_util:api_path(["clients", binary_to_list(ClientId2)]),
    {ok, _} = emqx_mgmt_api_test_util:request_api(delete, Client1Path, GoodHeaders),
    {ok, _} = emqx_mgmt_api_test_util:request_api(delete, Client2Path, GoodHeaders).

t_keepalive(Config) ->
    ClientFn = proplists:get_value(client_fn, Config),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    GoodHeaders = [AuthHeader, emqx_tenant_test_helpers:tenant_header(?HOST)],
    Username = "user_keepalive",
    ClientId = "client_keepalive",
    Path = emqx_mgmt_api_test_util:api_path(["clients", ClientId, "keepalive"]),
    Body = #{interval => 11},
    {error, {"HTTP/1.1", 404, "Not Found"}} =
        emqx_mgmt_api_test_util:request_api(put, Path, <<"">>, GoodHeaders, Body),
    {ok, C1} = ClientFn(ClientId, #{username => Username}),
    {error, {"HTTP/1.1", 404, "Not Found"}} =
        emqx_mgmt_api_test_util:request_api(put, Path, <<"">>, AuthHeader, Body),
    {ok, NewClient} = emqx_mgmt_api_test_util:request_api(put, Path, <<"">>, GoodHeaders, Body),
    #{<<"keepalive">> := 11} = emqx_json:decode(NewClient, [return_maps]),
    [Pid] = emqx_cm:lookup_channels({?TENANT, list_to_binary(ClientId)}),
    #{conninfo := #{keepalive := Keepalive}} = emqx_connection:info(Pid),
    ?assertEqual(11, Keepalive),
    emqtt:disconnect(C1),
    ok.

time_string_to_epoch_millisecond(DateTime) ->
    time_string_to_epoch(DateTime, millisecond).

time_string_to_epoch(DateTime, Unit) when is_binary(DateTime) ->
    try binary_to_integer(DateTime) of
        TimeStamp when is_integer(TimeStamp) -> TimeStamp
    catch
        error:badarg ->
            calendar:rfc3339_to_system_time(
                binary_to_list(DateTime), [{unit, Unit}]
            )
    end.
