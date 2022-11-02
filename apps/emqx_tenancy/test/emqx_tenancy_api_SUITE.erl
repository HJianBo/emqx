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

-include_lib("eunit/include/eunit.hrl").

all() -> [{group, parallel}, {group, sequence}].
suite() -> [{timetrap, {minutes, 1}}].
groups() ->
    [
        {parallel, [parallel], [t_create, t_update, t_delete]},
        {sequence, [], [t_create_failed]}
    ].

init_per_suite(Config) ->
    ok = application:load(emqx_tenancy),
    emqx_mgmt_api_test_util:init_suite(),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite().

t_create(_Config) ->
    Id = <<"emqx-tenent-id-1">>,
    {ok, Create} = create_tenant(Id),
    ?assertMatch(
        #{
            <<"quota">> := #{},
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
    ?assertEqual(Create, Tenant),
    ?assertEqual(
        {error, {"HTTP/1.1", 404, "Not Found"}}, read_tenant(<<"emqx-tenant-id-no-exist">>)
    ),
    ok.

t_create_failed(_Config) ->
    BadRequest = {error, {"HTTP/1.1", 400, "Bad Request"}},

    ?assertEqual(BadRequest, create_tenant(<<" error format name">>)),
    LongName = iolist_to_binary(lists:duplicate(257, "A")),
    ?assertEqual(BadRequest, create_tenant(LongName)),

    {ok, #{<<"data">> := List0}} = list_tenant([]),
    CreateNum = 30 - erlang:length(List0),
    Ids = lists:map(
        fun(Seq) ->
            <<"emqx-tenant-id-failed-", (integer_to_binary(Seq))/binary>>
        end,
        lists:seq(1, CreateNum)
    ),
    lists:foreach(fun(N) -> {ok, _} = create_tenant(N) end, Ids),
    ?assertEqual(BadRequest, create_tenant(<<"emqx-tenant-id-failed-1">>)),

    {ok, #{<<"data">> := List1}} = list_tenant("like_id=tenant-id-failed"),
    Ids1 = lists:map(fun(#{<<"id">> := Id}) -> Id end, List1),
    ?assertEqual(lists:sort(Ids), lists:sort(Ids1)),

    {ok, #{<<"data">> := List2}} = list_tenant("like_id=tenant-id-failedd"),
    ?assertEqual(0, length(List2)),

    lists:foreach(fun(N) -> {ok, _} = delete_tenant(N) end, Ids),
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
    {ok, _Create} = create_tenant(Id),
    {ok, Delete} = delete_tenant(Id),
    ?assertEqual([], Delete),
    ?assertEqual({ok, []}, delete_tenant(Id)),
    ok.

list_tenant(Qs) ->
    Path = emqx_mgmt_api_test_util:api_path(["tenants"]),
    Header = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(get, Path, Qs, Header) of
        {ok, Apps} -> {ok, emqx_json:decode(Apps, [return_maps])};
        Error -> Error
    end.

read_tenant(Id) ->
    Path = emqx_mgmt_api_test_util:api_path(["tenants", Id]),
    case emqx_mgmt_api_test_util:request_api(get, Path) of
        {ok, Res} -> {ok, emqx_json:decode(Res, [return_maps])};
        Error -> Error
    end.

create_tenant(Id) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["tenants"]),
    Tenant = #{
        id => Id,
        desc => <<"Note"/utf8>>,
        status => enabled
    },
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Tenant) of
        {ok, Res} -> {ok, emqx_json:decode(Res, [return_maps])};
        Error -> Error
    end.

delete_tenant(Id) ->
    DeletePath = emqx_mgmt_api_test_util:api_path(["tenants", Id]),
    emqx_mgmt_api_test_util:request_api(delete, DeletePath).

update_tenant(Id, Change) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    UpdatePath = emqx_mgmt_api_test_util:api_path(["tenants", Id]),
    case emqx_mgmt_api_test_util:request_api(put, UpdatePath, "", AuthHeader, Change) of
        {ok, Update} -> {ok, emqx_json:decode(Update, [return_maps])};
        Error -> Error
    end.
