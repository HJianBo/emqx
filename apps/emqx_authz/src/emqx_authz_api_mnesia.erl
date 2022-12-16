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

-module(emqx_authz_api_mnesia).

-behaviour(minirest_api).

-include("emqx_authz.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/1, mk/2, ref/1, ref/2, array/1, enum/1]).
-import(emqx_dashboard_utils, [tenant/1, tenant/2]).

-define(QUERY_USERNAME_FUN, fun ?MODULE:query_username/2).
-define(QUERY_CLIENTID_FUN, fun ?MODULE:query_clientid/2).

-define(ACL_USERNAME_QSCHEMA, [
    {<<"tenant_id">>, binary},
    {<<"like_username">>, binary}
]).
-define(ACL_CLIENTID_QSCHEMA, [
    {<<"tenant_id">>, binary},
    {<<"like_clientid">>, binary}
]).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1
]).

%% operation funs
-export([
    users/2,
    clients/2,
    user/2,
    client/2,
    all/2,
    purge/2
]).

%% query funs
-export([
    query_username/2,
    query_clientid/2,
    run_fuzzy_filter/2,
    format_result/1
]).

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(NOT_FOUND, 'NOT_FOUND').
-define(ALREADY_EXISTS, 'ALREADY_EXISTS').

-define(TYPE_REF, ref).
-define(TYPE_ARRAY, array).
-define(PAGE_QUERY_EXAMPLE, example_in_data).
-define(PUT_MAP_EXAMPLE, in_put_requestBody).
-define(POST_ARRAY_EXAMPLE, in_post_requestBody).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/authorization/sources/built_in_database/username",
        "/authorization/sources/built_in_database/clientid",
        "/authorization/sources/built_in_database/username/:username",
        "/authorization/sources/built_in_database/clientid/:clientid",
        "/authorization/sources/built_in_database/all",
        "/authorization/sources/built_in_database/purge-all"
    ].

%%--------------------------------------------------------------------
%% Schema for each URI
%%--------------------------------------------------------------------

schema("/authorization/sources/built_in_database/username") ->
    #{
        'operationId' => users,
        get =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(users_username_get),
                parameters =>
                    [
                        ref(emqx_dashboard_swagger, page),
                        ref(emqx_dashboard_swagger, limit),
                        ref(emqx_dashboard_swagger, tenant_id),
                        {like_username,
                            mk(binary(), #{
                                in => query,
                                required => false,
                                desc => ?DESC(fuzzy_username)
                            })}
                    ],
                responses =>
                    #{
                        200 => swagger_with_example(
                            {username_response_data, ?TYPE_REF},
                            {username, ?PAGE_QUERY_EXAMPLE}
                        )
                    }
            },
        post =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(users_username_post),
                parameters => [ref(emqx_dashboard_swagger, tenant_id)],
                'requestBody' => swagger_with_example(
                    {rules_for_username, ?TYPE_ARRAY},
                    {username, ?POST_ARRAY_EXAMPLE}
                ),
                responses =>
                    #{
                        204 => <<"Created">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad username or bad rule schema">>
                        ),
                        409 => emqx_dashboard_swagger:error_codes(
                            [?ALREADY_EXISTS], <<"ALREADY_EXISTS">>
                        )
                    }
            }
    };
schema("/authorization/sources/built_in_database/clientid") ->
    #{
        'operationId' => clients,
        get =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(users_clientid_get),
                parameters =>
                    [
                        ref(emqx_dashboard_swagger, page),
                        ref(emqx_dashboard_swagger, limit),
                        ref(emqx_dashboard_swagger, tenant_id),
                        {like_clientid,
                            mk(
                                binary(),
                                #{
                                    in => query,
                                    required => false,
                                    desc => ?DESC(fuzzy_clientid)
                                }
                            )}
                    ],
                responses =>
                    #{
                        200 => swagger_with_example(
                            {clientid_response_data, ?TYPE_REF},
                            {clientid, ?PAGE_QUERY_EXAMPLE}
                        )
                    }
            },
        post =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(users_clientid_post),
                parameters => [ref(emqx_dashboard_swagger, tenant_id)],
                'requestBody' => swagger_with_example(
                    {rules_for_clientid, ?TYPE_ARRAY},
                    {clientid, ?POST_ARRAY_EXAMPLE}
                ),
                responses =>
                    #{
                        204 => <<"Created">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad clientid or bad rule schema">>
                        )
                    }
            }
    };
schema("/authorization/sources/built_in_database/username/:username") ->
    #{
        'operationId' => user,
        get =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(user_username_get),
                parameters => [ref(username), ref(emqx_dashboard_swagger, tenant_id)],
                responses =>
                    #{
                        200 => swagger_with_example(
                            {rules_for_username, ?TYPE_REF},
                            {username, ?PUT_MAP_EXAMPLE}
                        ),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], <<"Not Found">>
                        )
                    }
            },
        put =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(user_username_put),
                parameters => [ref(username), ref(emqx_dashboard_swagger, tenant_id)],
                'requestBody' => swagger_with_example(
                    {rules_for_username, ?TYPE_REF},
                    {username, ?PUT_MAP_EXAMPLE}
                ),
                responses =>
                    #{
                        204 => <<"Updated">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad username or bad rule schema">>
                        )
                    }
            },
        delete =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(user_username_delete),
                parameters => [ref(username), ref(emqx_dashboard_swagger, tenant_id)],
                responses =>
                    #{
                        204 => <<"Deleted">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad username">>
                        ),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], <<"Username Not Found">>
                        )
                    }
            }
    };
schema("/authorization/sources/built_in_database/clientid/:clientid") ->
    #{
        'operationId' => client,
        get =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(user_clientid_get),
                parameters => [ref(clientid), ref(emqx_dashboard_swagger, tenant_id)],
                responses =>
                    #{
                        200 => swagger_with_example(
                            {rules_for_clientid, ?TYPE_REF},
                            {clientid, ?PUT_MAP_EXAMPLE}
                        ),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], <<"Not Found">>
                        )
                    }
            },
        put =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(user_clientid_put),
                parameters => [ref(clientid), ref(emqx_dashboard_swagger, tenant_id)],
                'requestBody' => swagger_with_example(
                    {rules_for_clientid, ?TYPE_REF},
                    {clientid, ?PUT_MAP_EXAMPLE}
                ),
                responses =>
                    #{
                        204 => <<"Updated">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad clientid or bad rule schema">>
                        )
                    }
            },
        delete =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(user_clientid_delete),
                parameters => [ref(clientid), ref(emqx_dashboard_swagger, tenant_id)],
                responses =>
                    #{
                        204 => <<"Deleted">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad clientid">>
                        ),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], <<"ClientID Not Found">>
                        )
                    }
            }
    };
schema("/authorization/sources/built_in_database/all") ->
    #{
        'operationId' => all,
        get =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(rules_for_all_get),
                parameters => [ref(emqx_dashboard_swagger, tenant_id)],
                responses =>
                    #{200 => swagger_with_example({rules, ?TYPE_REF}, {all, ?PUT_MAP_EXAMPLE})}
            },
        post =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(rules_for_all_post),
                parameters => [ref(emqx_dashboard_swagger, tenant_id)],
                'requestBody' =>
                    swagger_with_example({rules, ?TYPE_REF}, {all, ?PUT_MAP_EXAMPLE}),
                responses =>
                    #{
                        204 => <<"Updated">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad rule schema">>
                        )
                    }
            }
    };
schema("/authorization/sources/built_in_database/purge-all") ->
    #{
        'operationId' => purge,
        delete =>
            #{
                tags => [<<"authorization">>],
                description => ?DESC(purge_all_delete),
                responses =>
                    #{
                        204 => <<"Deleted">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad Request">>
                        )
                    }
            }
    }.

fields(rule_item) ->
    [
        {topic,
            mk(
                string(),
                #{
                    required => true,
                    desc => ?DESC(topic),
                    example => <<"test/topic/1">>
                }
            )},
        {permission,
            mk(
                enum([allow, deny]),
                #{
                    desc => ?DESC(permission),
                    required => true,
                    example => allow
                }
            )},
        {action,
            mk(
                enum([publish, subscribe, all]),
                #{
                    desc => ?DESC(action),
                    required => true,
                    example => publish
                }
            )}
    ];
fields(clientid) ->
    [
        {clientid,
            mk(
                binary(),
                #{
                    in => path,
                    required => true,
                    desc => ?DESC(clientid),
                    example => <<"client1">>
                }
            )}
    ];
fields(username) ->
    [
        {username,
            mk(
                binary(),
                #{
                    in => path,
                    required => true,
                    desc => ?DESC(username),
                    example => <<"user1">>
                }
            )}
    ];
fields(rules_for_username) ->
    fields(rules) ++
        fields(username);
fields(username_response_data) ->
    [
        {data, mk(array(ref(rules_for_username)), #{})},
        {meta, ref(emqx_dashboard_swagger, meta)}
    ];
fields(rules_for_clientid) ->
    fields(rules) ++
        fields(clientid);
fields(clientid_response_data) ->
    [
        {data, mk(array(ref(rules_for_clientid)), #{})},
        {meta, ref(emqx_dashboard_swagger, meta)}
    ];
fields(rules) ->
    [{rules, mk(array(ref(rule_item)))}].

%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------

users(get, Req = #{query_string := QueryString0}) ->
    Tenant = tenant(Req, undefined),
    QueryString = QueryString0#{<<"tenant_id">> => Tenant},
    case
        emqx_mgmt_api:node_query(
            node(),
            ?ACL_TABLE,
            QueryString,
            ?ACL_USERNAME_QSCHEMA,
            ?QUERY_USERNAME_FUN,
            fun ?MODULE:format_result/1
        )
    of
        {error, page_limit_invalid} ->
            {400, #{code => <<"INVALID_PARAMETER">>, message => <<"page_limit_invalid">>}};
        {error, Node, {badrpc, R}} ->
            Message = list_to_binary(io_lib:format("bad rpc call ~p, Reason ~p", [Node, R])),
            {500, #{code => <<"NODE_DOWN">>, message => Message}};
        Result ->
            {200, Result}
    end;
users(post, Req = #{body := Body}) when is_list(Body) ->
    Tenant = tenant(Req),
    case ensure_all_not_exists(Tenant, <<"username">>, username, Body) of
        [] ->
            with_acquire_quota(
                Tenant,
                count_rules_number(Body),
                fun() ->
                    lists:foreach(
                        fun(#{<<"username">> := Username, <<"rules">> := Rules}) ->
                            emqx_authz_mnesia:store_rules(
                                {username, Tenant, Username}, format_rules(Rules)
                            )
                        end,
                        Body
                    )
                end
            );
        Exists ->
            {409, #{
                code => <<"ALREADY_EXISTS">>,
                message => binfmt("Users '~ts' already exist", [binjoin(Exists)])
            }}
    end.

clients(get, Req = #{query_string := QueryString0}) ->
    Tenant = tenant(Req, undefined),
    QueryString = QueryString0#{<<"tenant_id">> => Tenant},
    case
        emqx_mgmt_api:node_query(
            node(),
            ?ACL_TABLE,
            QueryString,
            ?ACL_CLIENTID_QSCHEMA,
            ?QUERY_CLIENTID_FUN,
            fun ?MODULE:format_result/1
        )
    of
        {error, page_limit_invalid} ->
            {400, #{code => <<"INVALID_PARAMETER">>, message => <<"page_limit_invalid">>}};
        {error, Node, {badrpc, R}} ->
            Message = list_to_binary(io_lib:format("bad rpc call ~p, Reason ~p", [Node, R])),
            {500, #{code => <<"NODE_DOWN">>, message => Message}};
        Result ->
            {200, Result}
    end;
clients(post, Req = #{body := Body}) when is_list(Body) ->
    Tenant = tenant(Req),
    case ensure_all_not_exists(Tenant, <<"clientid">>, clientid, Body) of
        [] ->
            with_acquire_quota(
                Tenant,
                count_rules_number(Body),
                fun() ->
                    lists:foreach(
                        fun(#{<<"clientid">> := ClientID, <<"rules">> := Rules}) ->
                            emqx_authz_mnesia:store_rules(
                                {clientid, Tenant, ClientID}, format_rules(Rules)
                            )
                        end,
                        Body
                    )
                end
            );
        Exists ->
            {409, #{
                code => <<"ALREADY_EXISTS">>,
                message => binfmt("Clients '~ts' already exist", [binjoin(Exists)])
            }}
    end.

user(get, Req = #{bindings := #{username := Username}}) ->
    Tenant = tenant(Req),
    case emqx_authz_mnesia:lookup_rules({username, Tenant, Username}) of
        not_found ->
            {404, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}};
        {ok, Rules} ->
            {200, #{
                username => Username,
                rules => [
                    #{
                        topic => Topic,
                        action => Action,
                        permission => Permission
                    }
                 || {Permission, Action, Topic} <- Rules
                ]
            }}
    end;
user(
    put,
    Req = #{
        bindings := #{username := Username},
        body := #{<<"username">> := Username, <<"rules">> := Rules}
    }
) ->
    Tenant = tenant(Req),
    UpdateFun = fun() ->
        emqx_authz_mnesia:store_rules({username, Tenant, Username}, format_rules(Rules))
    end,
    case count_new_rules_number({username, Tenant, Username}, Rules) of
        Diff when Diff =:= 0 ->
            {204};
        Diff when Diff < 0 ->
            with_release_quota(Tenant, Diff, UpdateFun);
        Diff when Diff > 0 ->
            with_acquire_quota(Tenant, Diff, UpdateFun)
    end;
user(delete, Req = #{bindings := #{username := Username}}) ->
    Tenant = tenant(Req),
    case emqx_authz_mnesia:lookup_rules({username, Tenant, Username}) of
        not_found ->
            {404, #{code => <<"NOT_FOUND">>, message => <<"Username Not Found">>}};
        {ok, Rules} ->
            with_release_quota(
                Tenant,
                length(Rules),
                fun() ->
                    emqx_authz_mnesia:delete_rules({username, Tenant, Username})
                end
            )
    end.

client(get, Req = #{bindings := #{clientid := ClientID}}) ->
    Tenant = tenant(Req),
    case emqx_authz_mnesia:lookup_rules({clientid, Tenant, ClientID}) of
        not_found ->
            {404, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}};
        {ok, Rules} ->
            {200, #{
                clientid => ClientID,
                rules => [
                    #{
                        topic => Topic,
                        action => Action,
                        permission => Permission
                    }
                 || {Permission, Action, Topic} <- Rules
                ]
            }}
    end;
client(
    put,
    Req = #{
        bindings := #{clientid := ClientID},
        body := #{<<"clientid">> := ClientID, <<"rules">> := Rules}
    }
) ->
    Tenant = tenant(Req),
    UpdateFun = fun() ->
        emqx_authz_mnesia:store_rules({clientid, Tenant, ClientID}, format_rules(Rules))
    end,
    case count_new_rules_number({clientid, Tenant, ClientID}, Rules) of
        Diff when Diff =:= 0 ->
            {204};
        Diff when Diff < 0 ->
            with_release_quota(Tenant, Diff, UpdateFun);
        Diff when Diff > 0 ->
            with_acquire_quota(Tenant, Diff, UpdateFun)
    end;
client(delete, Req = #{bindings := #{clientid := ClientID}}) ->
    Tenant = tenant(Req),
    case emqx_authz_mnesia:lookup_rules({clientid, Tenant, ClientID}) of
        not_found ->
            {404, #{code => <<"NOT_FOUND">>, message => <<"ClientID Not Found">>}};
        {ok, Rules} ->
            with_release_quota(
                Tenant,
                length(Rules),
                fun() ->
                    emqx_authz_mnesia:delete_rules({clientid, Tenant, ClientID})
                end
            )
    end.

all(get, Req) ->
    Tenant = tenant(Req, undefined),
    Ms = emqx_authz_mnesia:list_all_rules(Tenant),
    Rules = ets:select(?ACL_TABLE, Ms),
    {200, #{
        rules =>
            [
                #{
                    topic => Topic,
                    action => Action,
                    permission => Permission
                }
             || {Permission, Action, Topic} <- lists:append(Rules)
            ]
    }};
all(post, Req = #{body := #{<<"rules">> := Rules}}) ->
    Tenant = tenant(Req),
    UpdateFun = fun() -> emqx_authz_mnesia:store_rules({all, Tenant}, format_rules(Rules)) end,
    with_acquire_quota(
        Tenant,
        count_new_rules_number({all, Tenant}, Rules),
        UpdateFun
    ).

purge(delete, _) ->
    case emqx_authz_api_sources:get_raw_source(<<"built_in_database">>) of
        [#{<<"enable">> := false}] ->
            ok = emqx_authz_mnesia:purge_rules(),
            {204};
        [#{<<"enable">> := true}] ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message =>
                    <<"'built_in_database' type source must be disabled before purge.">>
            }};
        [] ->
            {404, #{
                code => <<"BAD_REQUEST">>,
                message => <<"'built_in_database' type source is not found.">>
            }}
    end.

%%--------------------------------------------------------------------
%% Quotas

count_rules_number(Users) when is_list(Users) ->
    lists:sum(lists:map(fun(#{<<"rules">> := Rules}) -> length(Rules) end, Users)).

with_acquire_quota(Tenant, Count, Fun) ->
    case emqx_hooks:run_fold('quota.authz_rules', [{acquire, Count}, Tenant], allow) of
        allow ->
            %% assert
            ok = Fun(),
            {204};
        deny ->
            {409, #{
                code => <<"QUOTA_EXCEEDED">>,
                message => binfmt("Quota Exceeded")
            }}
    end.

with_release_quota(Tenant, Count, Fun) ->
    %% assert
    ok = Fun(),
    _ = emqx_hooks:run_fold('quota.authz_rules', [{release, Count}, Tenant], allow),
    {204}.

count_new_rules_number(Key, NewRules) ->
    Old =
        case emqx_authz_mnesia:lookup_rules(Key) of
            {ok, OldRules} -> length(OldRules);
            not_found -> 0
        end,
    length(NewRules) - Old.

%%--------------------------------------------------------------------
%% QueryString to MatchSpec

-spec query_username(atom(), {list(), list()}) -> emqx_mgmt_api:match_spec_and_filter().
query_username(_Tab, {QString, FuzzyQString}) ->
    {value, {tenant_id, _, Tenant}, _} = lists:keytake(tenant_id, 1, QString),
    #{
        match_spec => emqx_authz_mnesia:list_username_rules(Tenant),
        fuzzy_fun => fuzzy_filter_fun(FuzzyQString)
    }.

-spec query_clientid(atom(), {list(), list()}) -> emqx_mgmt_api:match_spec_and_filter().
query_clientid(_Tab, {QString, FuzzyQString}) ->
    {value, {tenant_id, _, Tenant}, _} = lists:keytake(tenant_id, 1, QString),
    #{
        match_spec => emqx_authz_mnesia:list_clientid_rules(Tenant),
        fuzzy_fun => fuzzy_filter_fun(FuzzyQString)
    }.

%% Fuzzy username funcs
fuzzy_filter_fun([]) ->
    undefined;
fuzzy_filter_fun(Fuzzy) ->
    {fun ?MODULE:run_fuzzy_filter/2, [Fuzzy]}.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(
    E = [{username, Username}, _Rule],
    [{username, like, UsernameSubStr} | Fuzzy]
) ->
    binary:match(Username, UsernameSubStr) /= nomatch andalso run_fuzzy_filter(E, Fuzzy);
run_fuzzy_filter(
    E = [{clientid, ClientId}, _Rule],
    [{clientid, like, ClientIdSubStr} | Fuzzy]
) ->
    binary:match(ClientId, ClientIdSubStr) /= nomatch andalso run_fuzzy_filter(E, Fuzzy).

%%--------------------------------------------------------------------
%% format funcs

%% format rule from api
format_rules(Rules) when is_list(Rules) ->
    lists:foldl(
        fun(
            #{
                <<"topic">> := Topic,
                <<"action">> := Action,
                <<"permission">> := Permission
            },
            AccIn
        ) when
            ?PUBSUB(Action) andalso
                ?ALLOW_DENY(Permission)
        ->
            AccIn ++ [{atom(Permission), atom(Action), Topic}]
        end,
        [],
        Rules
    ).

%% format result from mnesia tab
format_result([{username, Username}, {rules, Rules}]) ->
    #{
        username => Username,
        rules => [
            #{
                topic => Topic,
                action => Action,
                permission => Permission
            }
         || {Permission, Action, Topic} <- Rules
        ]
    };
format_result([{clientid, ClientID}, {rules, Rules}]) ->
    #{
        clientid => ClientID,
        rules => [
            #{
                topic => Topic,
                action => Action,
                permission => Permission
            }
         || {Permission, Action, Topic} <- Rules
        ]
    }.
atom(B) when is_binary(B) ->
    try
        binary_to_existing_atom(B, utf8)
    catch
        _Error:_Expection -> binary_to_atom(B)
    end;
atom(A) when is_atom(A) -> A.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

swagger_with_example({Ref, TypeP}, {_Name, _Type} = Example) ->
    emqx_dashboard_swagger:schema_with_examples(
        case TypeP of
            ?TYPE_REF -> ref(?MODULE, Ref);
            ?TYPE_ARRAY -> array(ref(?MODULE, Ref))
        end,
        rules_example(Example)
    ).

rules_example({ExampleName, ExampleType}) ->
    {Summary, Example} =
        case ExampleName of
            username -> {<<"Username">>, ?USERNAME_RULES_EXAMPLE};
            clientid -> {<<"ClientID">>, ?CLIENTID_RULES_EXAMPLE};
            all -> {<<"All">>, ?ALL_RULES_EXAMPLE}
        end,
    Value =
        case ExampleType of
            ?PAGE_QUERY_EXAMPLE ->
                #{
                    data => [Example],
                    meta => ?META_EXAMPLE
                };
            ?PUT_MAP_EXAMPLE ->
                Example;
            ?POST_ARRAY_EXAMPLE ->
                [Example]
        end,
    #{
        'password_based:built_in_database' => #{
            summary => Summary,
            value => Value
        }
    }.

ensure_all_not_exists(Tenant, Key, Type, Cfgs) ->
    lists:foldl(
        fun(#{Key := Id}, Acc) ->
            case emqx_authz_mnesia:lookup_rules({Type, Tenant, Id}) of
                not_found ->
                    Acc;
                _ ->
                    [Id | Acc]
            end
        end,
        [],
        Cfgs
    ).

binjoin([Bin]) ->
    Bin;
binjoin(Bins) ->
    binjoin(Bins, <<>>).

binjoin([H | T], Acc) ->
    binjoin(T, <<H/binary, $,, Acc/binary>>);
binjoin([], Acc) ->
    Acc.

binfmt(Fmt) -> binfmt(Fmt, []).
binfmt(Fmt, Args) -> iolist_to_binary(io_lib:format(Fmt, Args)).
