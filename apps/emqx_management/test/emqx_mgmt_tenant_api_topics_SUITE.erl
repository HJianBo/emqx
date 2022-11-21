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
-module(emqx_mgmt_tenant_api_topics_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-define(TENANT, <<"tenant_foo">>).
-define(HOST, <<?TENANT/binary, <<".emqxserver.io">>/binary>>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite(),
    emqx_config:put([tenant, tenant_id_from], peersni),
    ClientFn = emqx_tenant_test_helpers:reload_listener_with_ppv2(
        [listeners, tcp, default],
        ?HOST
    ),
    [{client_fn, ClientFn} | Config].

end_per_suite(_) ->
    emqx_tenant_test_helpers:reload_listener_without_ppv2([listeners, tcp, default]),
    emqx_mgmt_api_test_util:end_suite().

t_nodes_api(Config) ->
    ClientFn = proplists:get_value(client_fn, Config),
    {ok, Client} = ClientFn(<<"routes_cid">>, #{username => <<"routes_username">>}),
    Topic = <<"test_topic">>,
    {ok, _, _} = emqtt:subscribe(Client, Topic),

    %% list all
    Path = emqx_mgmt_api_test_util:api_path(["topics"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(get, Path, Auth),
    RoutesData = emqx_json:decode(Response, [return_maps]),
    Meta = maps:get(<<"meta">>, RoutesData),
    ?assertEqual(1, maps:get(<<"page">>, Meta)),
    ?assertEqual(emqx_mgmt:max_row_limit(), maps:get(<<"limit">>, Meta)),
    ?assertEqual(1, maps:get(<<"count">>, Meta)),
    Data = maps:get(<<"data">>, RoutesData),
    Route = erlang:hd(Data),
    ?assertEqual(
        emqx_mgmt_api_clients:warp_topic(Topic, ?TENANT),
        maps:get(<<"topic">>, Route)
    ),
    ?assertEqual(atom_to_binary(node(), utf8), maps:get(<<"node">>, Route)),

    Headers = [Auth, emqx_tenant_test_helpers:tenant_header(?HOST)],
    {ok, Response1} = emqx_mgmt_api_test_util:request_api(get, Path, Headers),
    RoutesData1 = emqx_json:decode(Response1, [return_maps]),
    Data1 = maps:get(<<"data">>, RoutesData1),
    Route1 = erlang:hd(Data1),
    ?assertEqual(Topic, maps:get(<<"topic">>, Route1)),

    %% bad tenant
    BadHeaders = [Auth, emqx_tenant_test_helpers:tenant_header(<<"bad.emqx.com">>)],
    {ok, Response2} = emqx_mgmt_api_test_util:request_api(get, Path, BadHeaders),
    ?assertMatch(#{<<"data">> := []}, emqx_json:decode(Response2, [return_maps])),

    %% get topics/:topic
    RoutePath = emqx_mgmt_api_test_util:api_path(["topics", Topic]),
    ?assertEqual(
        {error, {"HTTP/1.1", 404, "Not Found"}},
        emqx_mgmt_api_test_util:request_api(get, RoutePath),
        Auth
    ),
    {ok, RouteResponse} = emqx_mgmt_api_test_util:request_api(get, RoutePath, Headers),
    RouteData = emqx_json:decode(RouteResponse, [return_maps]),
    ?assertEqual(Topic, maps:get(<<"topic">>, RouteData)),
    ?assertEqual(atom_to_binary(node(), utf8), maps:get(<<"node">>, RouteData)).
