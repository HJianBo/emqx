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
-module(emqx_tenancy_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(
    emqx_dashboard_api_test_helpers,
    [
        uri/1,
        request/2,
        request/3,
        request_t/3,
        request_t/4
    ]
).

%%--------------------------------------------------------------------
%% setup
%%--------------------------------------------------------------------

all() -> [{group, parallel}, {group, sequence}].

suite() -> [{timetrap, {minutes, 1}}].

groups() ->
    [
        {parallel, [parallel], [t_create, t_update, t_delete]},
        {sequence, [], [t_create_failed, t_clear_resources_once_tenant_deleted]}
    ].

init_per_suite(Config) ->
    emqx_dashboard_api_test_helpers:init_suite(
        [emqx_conf, emqx_authn, emqx_authz, emqx_tenancy],
        fun set_special_configs/1
    ),
    Config.

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
    emqx_dashboard_api_test_helpers:end_suite([emqx_tenancy, emqx_authn, emqx_authz, emqx_conf]).

set_special_configs(emqx) ->
    %% restart gen_rpc with `stateless` mode
    application:set_env(gen_rpc, port_discovery, stateless),
    ok = application:stop(gen_rpc),
    ok = application:start(gen_rpc);
set_special_configs(emqx_authz) ->
    {ok, _} = emqx:update_config([authorization, sources], []);
set_special_configs(_) ->
    ok.

%%--------------------------------------------------------------------
%% tests
%%--------------------------------------------------------------------

t_create(_Config) ->
    Id = <<"emqx-tenent-id-1">>,
    {ok, Create} = create_tenant(Id),
    ?assertMatch(
        #{
            <<"configs">> := #{},
            <<"created_at">> := _,
            <<"updated_at">> := _,
            <<"desc">> := _,
            <<"status">> := <<"enabled">>,
            <<"id">> := Id
        },
        Create
    ),
    {ok, #{<<"data">> := List}} = list_tenant([]),
    ?assert(lists:member(Create, List)),
    {ok, Tenant} = read_tenant(Id),
    #{<<"configs">> := Configs} = Create,
    ?assertEqual(emqx_json:encode(Configs), emqx_json:encode(emqx_tenancy:default_configs())),
    ?assertEqual(Create, Tenant),
    ?assertMatch({ok, 404, _}, read_tenant(<<"emqx-tenant-id-no-exist">>)),
    ok.

t_create_failed(_Config) ->
    ?assertMatch({ok, 400, _}, create_tenant(<<"error format name">>)),
    LongName = iolist_to_binary(lists:duplicate(257, "A")),
    ?assertMatch({ok, 400, _}, create_tenant(LongName)),

    {ok, #{<<"data">> := List0}} = list_tenant([]),
    CreateNum = 30 - erlang:length(List0),
    Ids = lists:map(
        fun(Seq) ->
            <<"emqx-tenant-id-failed-", (integer_to_binary(Seq))/binary>>
        end,
        lists:seq(1, CreateNum)
    ),
    lists:foreach(fun(N) -> {ok, _} = create_tenant(N) end, Ids),
    ?assertMatch({ok, 400, _}, create_tenant(<<"emqx-tenant-id-failed-1">>)),

    {ok, #{<<"data">> := List1}} = list_tenant([{"like_id", "tenant-id-failed"}]),
    Ids1 = lists:map(fun(#{<<"id">> := Id}) -> Id end, List1),
    ?assertEqual(lists:sort(Ids), lists:sort(Ids1)),

    {ok, #{<<"data">> := List2}} = list_tenant([{"like_id", "tenant-id-failedd"}]),
    ?assertEqual(0, length(List2)),

    lists:foreach(fun(N) -> {ok, 204, _} = delete_tenant(N) end, Ids),
    ok.

t_update(_Config) ->
    Id = <<"emqx-tenant-id-update-key">>,
    {ok, _} = create_tenant(Id),

    Change = #{
        id => Id,
        desc => <<"NoteVersion1"/utf8>>,
        status => disabled
    },
    {ok, Update1} = update_tenant(Id, Change),
    ?assertEqual(Id, maps:get(<<"id">>, Update1)),
    ?assertEqual(<<"disabled">>, maps:get(<<"status">>, Update1)),
    ?assertEqual(<<"NoteVersion1"/utf8>>, maps:get(<<"desc">>, Update1)),
    ?assertEqual({error, {"HTTP/1.1", 400, "Bad Request"}}, update_tenant(<<"Not-Exist">>, Change)),
    ?assertEqual(
        {error, {"HTTP/1.1", 404, "Not Found"}},
        update_tenant(<<"Not-Exist">>, Change#{id => <<"Not-Exist">>})
    ),
    ok.

t_delete(_Config) ->
    Id = <<"emqx-tenant-id-delete">>,
    {ok, _} = create_tenant(Id),
    {ok, 204, _} = delete_tenant(Id),
    ?assertMatch({ok, 204, _}, delete_tenant(Id)),
    ok.

t_clear_resources_once_tenant_deleted(_Config) ->
    %% create authn/authz
    ?assertMatch({ok, 200, _}, create_built_in_authn()),
    ?assertMatch({ok, 204, _}, create_built_in_authz()),
    %% create tenants
    Tenant1 = <<"emqx-tenant-id-1">>,
    Tenant2 = <<"emqx-tenant-id-2">>,
    {ok, _} = create_tenant(Tenant1),
    {ok, _} = create_tenant(Tenant2),

    %% add authn users for tenants
    ?assertMatch({ok, 201, _}, add_authn_users(Tenant1, <<"username1">>, <<"pwd1">>)),
    ?assertMatch({ok, 201, _}, add_authn_users(Tenant2, <<"username2">>, <<"pwd2">>)),
    %% add authz rules for tenants
    Rules = [],
    ?assertMatch({ok, 204, _}, add_authz_rules(Tenant1, username, <<"username1">>, Rules)),
    ?assertMatch({ok, 204, _}, add_authz_rules(Tenant2, username, <<"username2">>, Rules)),

    %% delete tenant1, assert related users & rules deleted
    ?assertMatch({ok, 204, _}, delete_tenant(Tenant1)),
    {ok, 200, #{<<"data">> := Users1}} = list_authn_users(),
    {ok, 200, #{<<"data">> := Rules1}} = list_authz_rules(username),
    ?assertMatch([#{<<"user_id">> := <<"username2">>}], Users1),
    ?assertMatch([#{<<"username">> := <<"username2">>}], Rules1),

    %% delete tenant2, assert all users & rules deleted
    ?assertMatch({ok, 204, _}, delete_tenant(Tenant2)),
    {ok, 200, #{<<"data">> := Users2}} = list_authn_users(),
    {ok, 200, #{<<"data">> := Rules2}} = list_authz_rules(username),
    ?assertMatch([], Users2),
    ?assertMatch([], Rules2),
    %% remove authn/authz
    ?assertMatch({ok, 204, _}, remove_built_in_authn()),
    ?assertMatch({ok, 204, _}, remove_built_in_authz()).

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

list_tenant(Qs) when is_list(Qs) ->
    Uri = uri(["tenants"]) ++ "?" ++ uri_string:compose_query(Qs),
    {ok, 200, Res} = request(get, Uri),
    {ok, emqx_json:decode(Res, [return_maps])}.

read_tenant(Id) ->
    Uri = uri(["tenants", Id]),
    case request(get, Uri) of
        {ok, 200, Res} -> {ok, emqx_json:decode(Res, [return_maps])};
        {ok, Code, Resp} -> {ok, Code, Resp}
    end.

create_tenant(Id) ->
    Uri = uri(["tenants"]),
    Tenant = #{
        id => Id,
        desc => <<"Note"/utf8>>,
        status => enabled
    },
    case request(post, Uri, Tenant) of
        {ok, 201, Res} -> {ok, emqx_json:decode(Res, [return_maps])};
        {ok, Code, Res} -> {ok, Code, Res}
    end.

delete_tenant(Id) ->
    Uri = uri(["tenants", Id]),
    request(delete, Uri).

update_tenant(Id, Change) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    UpdatePath = emqx_mgmt_api_test_util:api_path(["tenants", Id]),
    case emqx_mgmt_api_test_util:request_api(put, UpdatePath, "", AuthHeader, Change) of
        {ok, Update} -> {ok, emqx_json:decode(Update, [return_maps])};
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% authn & authz helpers

create_built_in_authn() ->
    request(
        post,
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
    ).

create_built_in_authz() ->
    request(
        post,
        uri(["authorization", "sources"]),
        #{<<"type">> => <<"built_in_database">>}
    ).

add_authn_users(Tenant, UserId, Password) ->
    Body = #{
        user_id => UserId,
        password => Password,
        is_superuser => false
    },
    request_t(
        post,
        Tenant,
        uri(["authentication", "password_based:built_in_database", "users"]),
        Body
    ).

list_authn_users() ->
    list_authn_users(?NO_TENANT).

list_authn_users(Tenant) ->
    mayde_decode_data(
        request_t(
            get,
            Tenant,
            uri(["authentication", "password_based:built_in_database", "users"])
        )
    ).

add_authz_rules(Tenant, Type, Key, Rules) ->
    Body = [#{Type => Key, rules => Rules}],
    request_t(
        post,
        Tenant,
        uri(["authorization", "sources", "built_in_database", Type]),
        Body
    ).

list_authz_rules(Type) ->
    list_authz_rules(?NO_TENANT, Type).

list_authz_rules(Tenant, Type) ->
    mayde_decode_data(
        request_t(
            get,
            Tenant,
            uri(["authorization", "sources", "built_in_database", Type])
        )
    ).

remove_built_in_authn() ->
    request(delete, uri(["authentication", "password_based:built_in_database"])).

remove_built_in_authz() ->
    request(delete, uri(["authorization", "sources", "built_in_database"])).

mayde_decode_data({ok, Code, Resp}) ->
    {ok, Code, emqx_json:decode(Resp, [return_maps])};
mayde_decode_data(Result) ->
    Result.
