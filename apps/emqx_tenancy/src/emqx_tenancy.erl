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

-module(emqx_tenancy).

-include("emqx_tenancy.hrl").

%% API
-export([mnesia/1]).
-boot_mnesia({mnesia, [boot]}).

-export([load_tenants/0]).
-export([create/1, do_create/1]).
-export([read/1, do_read/1]).
-export([update/2, do_update/1]).
-export([delete/1, do_delete/1]).
-export([format/1]).

-ifdef(TEST).
-compile(nowarn_export_all).
-compile(export_all).
-endif.

-spec mnesia(boot) -> ok.
mnesia(boot) ->
    ok = mria:create_table(?TENANCY, [
        {type, set},
        {rlog_shard, ?COMMON_SHARD},
        {storage, disc_copies},
        {record_name, tenant},
        {attributes, record_info(fields, tenant)}
    ]).

%% @doc Load all tenants running resources
-spec load_tenants() -> ok.
load_tenants() ->
    load_tenants(ets:tab2list(?TENANCY)).
load_tenants([]) ->
    ok;
load_tenants([#tenant{id = Id, quota = Quota} | More]) ->
    ok = emqx_tenancy_limiter:do_create(Id, with_limiter_configs(Quota)),
    ok = emqx_tenancy_quota:do_create(Id, with_quota_config(Quota)),
    load_tenants(More).

-spec create(map()) -> {ok, map()} | {error, any()}.
create(Tenant) ->
    Now = now_second(),
    Tenant1 = to_tenant(Tenant),
    Tenant2 = Tenant1#tenant{created_at = Now, updated_at = Now},
    case
        emqx_tenancy_limiter:create(
            Tenant2#tenant.id,
            with_limiter_configs(Tenant2#tenant.quota)
        )
    of
        ok ->
            QuotaConfig = with_quota_config(Tenant2#tenant.quota),
            ok = emqx_tenancy_quota:create(Tenant2#tenant.id, QuotaConfig),
            trans(fun ?MODULE:do_create/1, [Tenant2]);
        {error, Reason} ->
            {error, Reason}
    end.

-spec read(tenant_id()) -> {ok, map()} | {error, any()}.
read(Id) ->
    trans(fun ?MODULE:do_read/1, [Id]).

-spec update(tenant_id(), map()) -> {ok, map()} | {error, any()}.
update(Id, #{<<"id">> := Id} = Tenant) ->
    Config = maps:get(<<"quota">>, Tenant),
    case
        emqx_tenancy_limiter:update(
            Id,
            with_limiter_configs(Config)
        )
    of
        ok ->
            QuotaConfig = with_quota_config(Config),
            ok = emqx_tenancy_quota:update(Id, QuotaConfig),
            trans(fun ?MODULE:do_update/1, [Tenant]);
        {error, Reason} ->
            {error, Reason}
    end;
update(_Id, _) ->
    {error, invalid_tenant}.

-spec delete(tenant_id()) -> ok.
delete(Id) ->
    ok = emqx_tenancy_limiter:remove(Id),
    ok = emqx_tenancy_quota:remove(Id),
    trans(fun ?MODULE:do_delete/1, [Id]).

trans(Fun, Args) ->
    case mria:transaction(?COMMON_SHARD, Fun, Args) of
        {atomic, ok} -> ok;
        {atomic, Res = #tenant{}} -> {ok, format(Res)};
        {aborted, Error} -> {error, Error}
    end.

do_create(Tenant = #tenant{id = Id}) ->
    case mnesia:read(?TENANCY, Id, read) of
        [_] ->
            mnesia:abort(already_existed);
        [] ->
            ok = mnesia:write(?TENANCY, Tenant, write),
            Tenant
    end.

do_read(Id) ->
    case mnesia:read(?TENANCY, Id, read) of
        [] -> mnesia:abort(not_found);
        [Tenant] -> Tenant
    end.

do_update(Tenant = #{<<"id">> := Id}) ->
    case mnesia:read(?TENANCY, Id, write) of
        [] ->
            mnesia:abort(not_found);
        [Prev] ->
            #tenant{
                quota = Quota,
                status = Status,
                created_at = CreatedAt,
                desc = Desc
            } = Prev,
            NewTenant = Prev#tenant{
                desc = maps:get(<<"desc">>, Tenant, Desc),
                quota = maps:get(<<"quota">>, Tenant, Quota),
                status = maps:get(<<"status">>, Tenant, Status),
                created_at = CreatedAt,
                updated_at = now_second()
            },
            ok = mnesia:write(?TENANCY, NewTenant, write),
            NewTenant
    end.

do_delete(Id) ->
    mnesia:delete(?TENANCY, Id, write),
    %% delete authn/authz related users
    emqx_authn_mnesia:do_tenant_deleted(Id),
    emqx_authz_mnesia:do_tenant_deleted(Id).

format(Tenants) when is_list(Tenants) ->
    [format(Tenant) || Tenant <- Tenants];
format(#tenant{
    id = Id,
    quota = Quota,
    status = Status,
    updated_at = UpdatedAt,
    created_at = CreatedAt,
    desc = Desc
}) ->
    #{
        id => Id,
        quota => Quota,
        status => Status,
        created_at => to_rfc3339(CreatedAt),
        updated_at => to_rfc3339(UpdatedAt),
        desc => Desc
    }.

to_rfc3339(Second) ->
    list_to_binary(calendar:system_time_to_rfc3339(Second)).

to_tenant(Tenant) ->
    #{
        <<"id">> := Id,
        <<"quota">> := Quota0,
        <<"status">> := Status,
        <<"desc">> := Desc
    } = Tenant,
    Quota1 = maps:merge(default_quota(), Quota0),
    #tenant{
        id = Id,
        quota = Quota1,
        status = Status,
        desc = Desc
    }.

default_quota() ->
    #{
        <<"max_connection">> => 10000,
        <<"max_conn_rate">> => 1000,
        <<"max_messages_in">> => 1000,
        <<"max_bytes_in">> => 100000,
        <<"max_subs_rate">> => 500,
        <<"max_authn_users">> => 10000,
        <<"max_authz_users">> => 10000,
        <<"max_retained_messages">> => 1000,
        <<"max_rules">> => 1000,
        <<"max_resources">> => 50,
        <<"max_shared_subscriptions">> => 100,
        <<"min_keepalive">> => 30,
        <<"max_keepalive">> => 3600,
        <<"session_expiry_interval">> => 7200,
        <<"max_mqueue_len">> => 32,
        <<"max_inflight">> => 100,
        <<"max_awaiting_rel">> => 100,
        <<"max_subscriptions">> => infinity,
        <<"max_packet_size">> => 1048576,
        <<"max_clientid_len">> => 65535,
        <<"max_topic_levels">> => 65535,
        <<"max_qos_allowed">> => 2,
        <<"max_topic_alias">> => 65535
    }.

with_limiter_configs(Config) when is_map(Config) ->
    Keys = [
        <<"max_conn_rate">>,
        <<"max_messages_in">>,
        <<"max_bytes_in">>
    ],
    emqx_map_lib:safe_atom_key_map(maps:with(Keys, Config)).

with_quota_config(Config) when is_map(Config) ->
    Keys = [
        <<"max_connection">>,
        <<"max_authn_users">>,
        <<"max_authz_users">>,
        <<"max_subscriptions">>,
        <<"max_retained_messages">>,
        <<"max_rules">>,
        <<"max_resources">>,
        <<"max_shared_subscriptions">>
    ],
    emqx_map_lib:safe_atom_key_map(maps:with(Keys, Config)).

now_second() ->
    os:system_time(second).
