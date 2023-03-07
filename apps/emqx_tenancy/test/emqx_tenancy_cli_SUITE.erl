%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_tenancy_cli_SUITE).

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

all() -> emqx_common_test_helpers:all(?MODULE).

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
    _ = application:stop(gen_rpc),
    timer:sleep(100),
    ok = application:start(gen_rpc);
set_special_configs(emqx_authz) ->
    {ok, _} = emqx:update_config([authorization, sources], []);
set_special_configs(_) ->
    ok.

%%--------------------------------------------------------------------
%% tests
%%--------------------------------------------------------------------

t_export_import(_Config) ->
    Id = <<"emqx-tenent-id-1">>,
    %% create authn/authz first
    ?assertMatch({ok, 200, _}, create_built_in_authn()),
    ?assertMatch({ok, 204, _}, create_built_in_authz()),
    %% create tenants
    ?assertMatch({ok, _}, create_tenant(Id)),
    %% add authn users for tenants
    ?assertMatch({ok, 201, _}, add_authn_users(Id, <<"username1">>, <<"pwd1">>)),
    ?assertMatch({ok, 201, _}, add_authn_users(Id, <<"username2">>, <<"pwd2">>)),
    %% add authz rules for tenants
    Rules = [],
    ?assertMatch({ok, 204, _}, add_authz_rules(Id, username, <<"username1">>, Rules)),
    ?assertMatch({ok, 204, _}, add_authz_rules(Id, username, <<"username2">>, Rules)),

    monitor_ctl_print(),

    %% export
    ok = emqx_tenancy_cli:'tenants-data'(["export", "all"]),
    "Exporting to " ++ Path1 = recv_ctl_print(),
    ?assertMatch("Exported 1 tenants, 2 authn records, 2 authz records" ++ _, recv_ctl_print()),
    %% export twice
    ok = emqx_tenancy_cli:'tenants-data'(["export", "all"]),
    "Exporting to " ++ Path2 = recv_ctl_print(),
    ?assertMatch("Exported 1 tenants, 2 authn records, 2 authz records" ++ _, recv_ctl_print()),

    ?assertNotEqual(Path1, Path2),

    %% delete tenant1, assert related users & rules deleted
    ?assertMatch({ok, 204, _}, delete_tenant(Id)),
    ?assertMatch({ok, 200, #{<<"data">> := []}}, list_authn_users()),
    ?assertMatch({ok, 200, #{<<"data">> := []}}, list_authz_rules(username)),

    %% import
    Path = string:trim(Path2),
    ok = emqx_tenancy_cli:'tenants-data'(["import", Path]),
    ?assertEqual("Importing from " ++ Path ++ "\n", recv_ctl_print()),
    ?assertMatch("Try to import 1 tenant records..\n", recv_ctl_print()),
    ?assertMatch("  succeed: 1, existed: 0, failed: 0\n", recv_ctl_print()),
    ?assertMatch("Try to import 2 authn records..\n", recv_ctl_print()),
    ?assertMatch("  succeed: 2, existed: 0, failed: 0\n", recv_ctl_print()),
    ?assertMatch("Try to import 2 authz records..\n", recv_ctl_print()),
    ?assertMatch("  succeed: 2, existed: 0, failed: 0\n", recv_ctl_print()),
    ?assertMatch("Done\n", recv_ctl_print()),
    %% import twice
    ok = emqx_tenancy_cli:'tenants-data'(["import", Path]),
    ?assertEqual("Importing from " ++ Path ++ "\n", recv_ctl_print()),
    ?assertMatch("Try to import 1 tenant records..\n", recv_ctl_print()),
    ?assertMatch("  succeed: 0, existed: 1, failed: 0\n", recv_ctl_print()),
    ?assertMatch("Try to import 2 authn records..\n", recv_ctl_print()),
    ?assertMatch("  succeed: 0, existed: 2, failed: 0\n", recv_ctl_print()),
    ?assertMatch("Try to import 2 authz records..\n", recv_ctl_print()),
    ?assertMatch("  succeed: 0, existed: 2, failed: 0\n", recv_ctl_print()),
    ?assertMatch("Done\n", recv_ctl_print()),
    %% verify the imported records
    {ok, #{<<"data">> := List1}} = list_tenant([]),
    ?assertEqual(1, length(List1)),
    {ok, 200, #{<<"data">> := Users1}} = list_authn_users(),
    {ok, 200, #{<<"data">> := Rules1}} = list_authz_rules(username),
    ?assertEqual(2, length(Users1)),
    ?assertEqual(2, length(Rules1)),

    demonitor_ctl_print(),

    %% clear tenants, authn, authz records
    ?assertMatch({ok, 204, _}, delete_tenant(Id)),
    ?assertMatch({ok, 200, #{<<"data">> := []}}, list_authn_users()),
    ?assertMatch({ok, 200, #{<<"data">> := []}}, list_authz_rules(username)),
    %% remove authn/authz
    ?assertMatch({ok, 204, _}, remove_built_in_authn()),
    ?assertMatch({ok, 204, _}, remove_built_in_authz()).

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

demonitor_ctl_print() ->
    ok = meck:unload([emqx_ctl]).

monitor_ctl_print() ->
    _Self = self(),
    ok = meck:new(emqx_ctl, [passthrough, no_history]),
    ok = meck:expect(
        emqx_ctl,
        print,
        fun(Fmt, Args) ->
            _Self ! {print, lists:flatten(io_lib:format(Fmt, Args))},
            meck:passthrough([Fmt, Args])
        end
    ),
    ok = meck:expect(
        emqx_ctl,
        print,
        fun(Fmt) ->
            _Self ! {print, lists:flatten(io_lib:format(Fmt, []))},
            meck:passthrough([Fmt])
        end
    ).

recv_ctl_print() ->
    receive
        {print, Msg} -> Msg
    after 5000 -> error(timeout)
    end.

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
        enabled => true
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
