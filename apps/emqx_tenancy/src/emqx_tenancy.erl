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

%% Internal exports
-export([with_quota_config/1, with_limiter_configs/1]).

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
load_tenants([#tenant{id = Id, configs = Quota} | More]) ->
    ok = emqx_tenancy_limiter:do_create(Id, with_limiter_configs(Quota)),
    load_tenants(More).

-spec create(map()) -> {ok, map()} | {error, any()}.
create(Tenant) ->
    Now = now_second(),
    Tenant1 = to_tenant(Tenant),
    Tenant2 = Tenant1#tenant{created_at = Now, updated_at = Now},
    case
        emqx_tenancy_limiter:create(
            Tenant2#tenant.id,
            with_limiter_configs(Tenant2#tenant.configs)
        )
    of
        ok ->
            QuotaConfig = with_quota_config(Tenant2#tenant.configs),
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
    trans(fun ?MODULE:do_update/1, [Tenant]);
update(_Id, _) ->
    {error, invalid_tenant}.

-spec delete(tenant_id()) -> ok.
delete(Id) ->
    ok = emqx_tenancy_quota:remove(Id),
    ok = emqx_tenancy_limiter:remove(Id),
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

do_update(Update = #{<<"id">> := Id}) ->
    case mnesia:read(?TENANCY, Id, write) of
        [] ->
            mnesia:abort(not_found);
        [Prev] ->
            #tenant{
                configs = Configs,
                enabled = Enabled,
                created_at = CreatedAt,
                desc = Desc
            } = Prev,
            NewConfigs = emqx_map_lib:deep_merge(Configs, maps:get(<<"configs">>, Update, Configs)),
            case
                emqx_tenancy_limiter:update(
                    Id,
                    with_limiter_configs(NewConfigs)
                )
            of
                ok ->
                    NewQuotaConfigs = with_quota_config(NewConfigs),
                    ok = emqx_tenancy_quota:update(Id, NewQuotaConfigs),

                    NewTenant = Prev#tenant{
                        desc = maps:get(<<"desc">>, Update, Desc),
                        configs = NewConfigs,
                        enabled = maps:get(<<"enabled">>, Update, Enabled),
                        created_at = CreatedAt,
                        updated_at = now_second()
                    },
                    ok = mnesia:write(?TENANCY, NewTenant, write),
                    NewTenant;
                {error, Reason} ->
                    mnesia:abort(Reason)
            end
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
    configs = Configs,
    enabled = Enabled,
    updated_at = UpdatedAt,
    created_at = CreatedAt,
    desc = Desc
}) ->
    #{
        id => Id,
        configs => Configs,
        enabled => Enabled,
        created_at => to_rfc3339(CreatedAt),
        updated_at => to_rfc3339(UpdatedAt),
        desc => Desc
    }.

to_rfc3339(Second) ->
    list_to_binary(calendar:system_time_to_rfc3339(Second)).

to_tenant(Tenant) ->
    #{
        <<"id">> := Id,
        <<"configs">> := Configs,
        <<"enabled">> := Enabled,
        <<"desc">> := Desc
    } = Tenant,
    Configs1 = emqx_map_lib:deep_merge(default_configs(), Configs),
    #tenant{
        id = Id,
        configs = Configs1,
        enabled = Enabled,
        desc = Desc
    }.

default_configs() ->
    #{
        <<"quotas">> => #{
            <<"max_sessions">> => 1000,
            <<"max_authn_users">> => 2000,
            <<"max_authz_rules">> => 2000
        },
        <<"limiters">> => #{
            <<"max_messages_in">> => 1000,
            <<"max_bytes_in">> => 10 * 1024 * 1024
        }
    }.

with_limiter_configs(Config0) when is_map(Config0) ->
    Keys = [
        <<"max_messages_in">>,
        <<"max_bytes_in">>
    ],
    Config = maps:get(<<"limiters">>, Config0),
    emqx_map_lib:safe_atom_key_map(maps:with(Keys, Config)).

with_quota_config(Config0) when is_map(Config0) ->
    Keys = [
        <<"max_sessions">>,
        <<"max_authn_users">>,
        <<"max_authz_rules">>
    ],
    Config = maps:get(<<"quotas">>, Config0),
    emqx_map_lib:safe_atom_key_map(maps:with(Keys, Config)).

now_second() ->
    os:system_time(second).
