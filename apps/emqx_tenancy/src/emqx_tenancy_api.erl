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

-module(emqx_tenancy_api).

-behaviour(minirest_api).

-include("emqx_tenancy.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([api_spec/0, fields/1, paths/0, schema/1, namespace/0]).
-export([tenants/2, tenant_with_name/2]).
-export([validate_id/1]).

%% query
-export([qs2ms/2, run_fuzzy_filter/2]).

-define(TAGS, [<<"Tenants">>]).
-define(NOT_FOUND_RESPONSE, #{code => 'NOT_FOUND', message => <<"Name NOT FOUND">>}).

-define(QS_SCHEMA, [
    {<<"enabled">>, atom},
    {<<"like_id">>, binary},
    {<<"gte_created_at">>, timestamp},
    {<<"lte_created_at">>, timestamp},
    {<<"gte_updated_at">>, timestamp},
    {<<"lte_updated_at">>, timestamp}
]).

-define(QUERY_FUN, {?MODULE, query}).

namespace() -> "tenant".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    ["/tenants", "/tenants/:id"].

schema("/tenants") ->
    #{
        'operationId' => tenants,
        get => #{
            description => "Return tenants list",
            tags => ?TAGS,
            parameters => [
                {like_id,
                    hoconsc:mk(binary(), #{
                        in => query,
                        required => false,
                        desc => <<"Fuzzy search `tenant id` as substring">>
                    })},
                {gte_created_at,
                    hoconsc:mk(emqx_datetime:epoch_second(), #{
                        in => query,
                        required => false,
                        desc =>
                            <<"Search creation time by greater",
                                " than or equal method, rfc3339 or timestamp(millisecond)">>
                    })},
                {lte_created_at,
                    hoconsc:mk(emqx_datetime:epoch_second(), #{
                        in => query,
                        required => false,
                        desc =>
                            <<"Search creation time by less",
                                " than or equal method, rfc3339 or timestamp(millisecond)">>
                    })},
                {gte_updated_at,
                    hoconsc:mk(emqx_datetime:epoch_second(), #{
                        in => query,
                        required => false,
                        desc =>
                            <<"Search update time by greater",
                                " than or equal method, rfc3339 or timestamp(millisecond)">>
                    })},
                {lte_updated_at,
                    hoconsc:mk(emqx_datetime:epoch_second(), #{
                        in => query,
                        required => false,
                        desc =>
                            <<"Search update time by less",
                                " than or equal method, rfc3339 or timestamp(millisecond)">>
                    })},
                hoconsc:ref(emqx_dashboard_swagger, page),
                hoconsc:ref(emqx_dashboard_swagger, limit)
            ],
            responses => #{
                200 =>
                    [
                        {data, ?HOCON(?ARRAY(?REF(tenant_resp)), #{})},
                        {meta, ?HOCON(?R_REF(emqx_dashboard_swagger, meta), #{})}
                    ]
            }
        },
        post => #{
            description => "Create new tenant",
            tags => ?TAGS,
            'requestBody' => ?REF(tenant_req),
            responses => #{
                201 => ?REF(tenant_resp),
                400 => emqx_dashboard_swagger:error_codes(['BAD_REQUEST'])
            }
        }
    };
schema("/tenants/:id") ->
    #{
        'operationId' => tenant_with_name,
        get => #{
            description => "Return the specific tenant",
            tags => ?TAGS,
            parameters => [?REF(id)],
            responses => #{
                200 => ?REF(tenant_resp),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'])
            }
        },
        put => #{
            description => "Update the specific tenant",
            tags => ?TAGS,
            parameters => [?REF(id)],
            'requestBody' => ?REF(tenant_req),
            responses => #{
                200 => ?REF(tenant_resp),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND']),
                400 => emqx_dashboard_swagger:error_codes(['BAD_REQUEST'])
            }
        },
        delete => #{
            description => "Delete the specific tenant",
            tags => ?TAGS,
            parameters => [?REF(id)],
            responses => #{
                204 => <<"Delete successfully">>,
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'])
            }
        }
    }.

fields(tenant_req) ->
    delete([created_at, updated_at], tenant_field(false));
fields(tenant_resp) ->
    tenant_field(true);
fields(id) ->
    [
        {id,
            ?HOCON(
                binary(),
                #{
                    desc => <<"^[A-Za-z]+[A-Za-z0-9-_]*$">>,
                    example => <<"emqx-tenant-id">>,
                    in => path,
                    validator => fun ?MODULE:validate_id/1
                }
            )}
    ];
fields(tenant_configs) ->
    [
        {quotas, ?HOCON(?REF(quotas))},
        {limiters, ?HOCON(?REF(limiters))}
        %% TODO: 2.0
        %{min_keepalive, ?HOCON(integer(), #{desc => <<"Min keepalive second">>, default => 30})},
        %{max_keepalive, ?HOCON(integer(), #{desc => <<"Max keepalive">>, default => 3600})},
        %{session_expiry_interval,
        %    ?HOCON(integer(), #{desc => <<"Session expiry interval">>, default => 7200})},
        %{max_mqueue_len, ?HOCON(integer(), #{desc => <<"Max mqueue len">>, default => 32})},
        %{max_inflight, ?HOCON(integer(), #{desc => <<"Max inflight">>, default => 100})},
        %{max_awaiting_rel, ?HOCON(integer(), #{desc => <<"Max awaiting rel">>, default => 100})},
        %{max_packet_size, ?HOCON(integer(), #{desc => <<"Max packet size">>, default => 1048576})},
        %{max_clientid_len, ?HOCON(integer(), #{desc => <<"Max clientid len">>, default => 65535})},
        %{max_topic_levels, ?HOCON(integer(), #{desc => <<"Max topic levels">>, default => 65535})},
        %{max_qos_allowed, ?HOCON(integer(), #{desc => <<"Max qos allowed">>, default => 2})},
        %{max_topic_alias, ?HOCON(integer(), #{desc => <<"Max topic alias">>, default => 65535})}
        %force_shutdown_policy object {"max_message_queue_len": 1000,"max_heap_size": "32MB"}
    ];
fields(quotas) ->
    [
        {max_sessions, ?HOCON(integer(), #{desc => <<"Max sessions">>, default => 1000})},
        {max_authn_users, ?HOCON(integer(), #{desc => <<"Max authn users">>, default => 2000})},
        {max_authz_rules, ?HOCON(integer(), #{desc => <<"Max authz users">>, default => 2000})}
        %% TODO: 2.0
        %{max_retained_messages,
        %    ?HOCON(integer(), #{desc => <<"Max retained messages">>, default => 1000})},
        %{max_rules, ?HOCON(integer(), #{desc => <<"Max rules">>, default => 1000})},
        %{max_resources, ?HOCON(integer(), #{desc => <<"Max resources">>, default => 50})},
        %{max_subscriptions,
        %    ?HOCON(?UNION([integer(), infinity]), #{
        %        desc => <<"Max subscriptions">>, default => infinity
        %    })},
        %{max_shared_subscriptions,
        %    ?HOCON(integer(), #{desc => <<"Max shared subscriptions">>, default => 100})},
    ];
fields(limiters) ->
    [
        {max_messages_in, ?HOCON(integer(), #{desc => <<"Max messages in">>, default => 1000})},
        {max_bytes_in,
            ?HOCON(integer(), #{desc => <<"Max bytes in">>, default => 10 * 1024 * 1024})}
        %% TODO: 2.0
        %{max_conn_rate, ?HOCON(integer(), #{desc => <<"Max connection rate">>, default => 100})},
        %{max_subs_rate, ?HOCON(integer(), #{desc => <<"Max subscriptions rate">>, default => 500})},
    ].

tenant_field(Required) ->
    [
        {id,
            ?HOCON(
                binary(),
                #{
                    desc => "Unique and format by ^[A-Za-z]+[A-Za-z0-9-_]*$",
                    validator => fun ?MODULE:validate_id/1,
                    required => true,
                    example => <<"emqx-tenant-id">>
                }
            )},
        {configs, ?HOCON(?REF(tenant_configs), #{required => Required})},
        {enabled,
            ?HOCON(
                ?ENUM([true, false]),
                #{
                    desc => "Whether to enable this tenant",
                    required => Required,
                    default => true
                }
            )},
        {desc,
            ?HOCON(binary(), #{
                desc => "Description of the tenant",
                required => Required,
                example => "This is a tenant for emqx"
            })},
        {created_at,
            hoconsc:mk(
                emqx_datetime:epoch_second(),
                #{
                    desc => "tentant create datetime",
                    example => <<"2022-12-01T00:00:00.000Z">>,
                    required => Required
                }
            )},
        {updated_at,
            hoconsc:mk(
                emqx_datetime:epoch_second(),
                #{
                    desc => "tentant update datetime",
                    example => <<"2022-12-01T00:00:00.000Z">>,
                    required => Required
                }
            )}
    ].

delete(Keys, Fields) ->
    lists:foldl(fun(Key, Acc) -> lists:keydelete(Key, 1, Acc) end, Fields, Keys).

-define(ID_RE, "^[A-Za-z]+[A-Za-z0-9-_]*$").

validate_id(Sni) ->
    IdLen = byte_size(Sni),
    case IdLen > 0 andalso IdLen =< 256 of
        true ->
            case re:run(Sni, ?ID_RE) of
                nomatch -> {error, "id should be " ?ID_RE};
                _ -> ok
            end;
        false ->
            {error, "id Length must =< 256"}
    end.

tenants(get, #{query_string := Qs}) ->
    case
        emqx_mgmt_api:node_query(
            node(),
            ?TENANCY,
            Qs,
            ?QS_SCHEMA,
            fun ?MODULE:qs2ms/2,
            fun emqx_tenancy:format/1
        )
    of
        {error, page_limit_invalid} ->
            {400, #{code => <<"INVALID_PARAMETER">>, message => <<"page_limit_invalid">>}};
        Response ->
            {200, Response}
    end;
tenants(post, #{body := Tenant}) ->
    #{<<"desc">> := Desc0} = Tenant,
    Desc = unicode:characters_to_binary(Desc0, unicode),
    case emqx_tenancy:create(Tenant#{<<"desc">> => Desc}) of
        {ok, NewTenant} ->
            {201, NewTenant};
        {error, Reason} ->
            {400, #{
                code => 'BAD_REQUEST',
                message => iolist_to_binary(io_lib:format("~p", [Reason]))
            }}
    end.

tenant_with_name(get, #{bindings := #{id := Id}}) ->
    case emqx_tenancy:read(Id) of
        {ok, Tenant} -> {200, Tenant};
        {error, not_found} -> {404, ?NOT_FOUND_RESPONSE}
    end;
tenant_with_name(delete, #{bindings := #{id := Id}}) ->
    ok = emqx_tenancy:delete(Id),
    {204};
tenant_with_name(put, #{bindings := #{id := Id}, body := Body}) ->
    case emqx_tenancy:update(Id, Body) of
        {ok, Tenant} -> {200, Tenant};
        {error, invalid_tenant} -> {400, #{code => 'BAD_REQUEST', message => <<"invalid_tenant">>}};
        {error, {_Node, not_found}} -> {404, ?NOT_FOUND_RESPONSE}
    end.

%%--------------------------------------------------------------------
%% QueryString to Match Spec

-spec qs2ms(atom(), {list(), list()}) -> emqx_mgmt_api:match_spec_and_filter().
qs2ms(_Tab, {Qs, Fuzzy}) ->
    #{
        match_spec => qs2ms(Qs),
        fuzzy_fun => fuzzy_filter_fun(Fuzzy)
    }.

qs2ms(Qs) ->
    {MatchHead, Cond} = qs2ms(Qs, 2, {#tenant{_ = '_'}, []}),
    [{MatchHead, Cond, ['$_']}].

qs2ms([], _, {MatchHead, Cond}) ->
    {MatchHead, lists:reverse(Cond)};
qs2ms([{enabled, '=:=', Status} | Rest], N, {MatchHead, Cond}) ->
    NMatchHead = MatchHead#tenant{enabled = Status},
    qs2ms(Rest, N, {NMatchHead, Cond});
qs2ms([Qs | Rest], N, {MatchHead, Cond}) when
    element(1, Qs) =:= updated_at orelse element(1, Qs) =:= created_at
->
    Holder = binary_to_atom(iolist_to_binary(["$", integer_to_list(N)]), utf8),
    NMatchHead = MatchHead#tenant{updated_at = Holder},
    NCond = put_cond(Qs, Holder, Cond),
    qs2ms(Rest, N + 1, {NMatchHead, NCond}).

put_cond({_, Op, V}, Holder, Cond) ->
    [{Op, Holder, V} | Cond];
put_cond({_, Op1, V1, Op2, V2}, Holder, Cond) ->
    [
        {Op2, Holder, V2},
        {Op1, Holder, V1}
        | Cond
    ].

%%--------------------------------------------------------------------
%% Match funcs

fuzzy_filter_fun([]) ->
    undefined;
fuzzy_filter_fun(Fuzzy) ->
    {fun ?MODULE:run_fuzzy_filter/2, [Fuzzy]}.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(
    E = #tenant{id = Id},
    [{id, like, SubId} | Fuzzy]
) ->
    binary:match(Id, SubId) /= nomatch andalso run_fuzzy_filter(E, Fuzzy).