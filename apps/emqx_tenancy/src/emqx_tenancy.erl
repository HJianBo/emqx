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

-export([create/1, do_create/1]).
-export([read/1, do_read/1]).
-export([update/2, do_update/1]).
-export([delete/1, do_delete/1]).
-export([format/1]).

-spec mnesia(boot) -> ok.
mnesia(boot) ->
    ok = mria:create_table(?TENANCY, [
        {type, set},
        {rlog_shard, ?COMMON_SHARD},
        {storage, disc_copies},
        {record_name, tenant},
        {attributes, record_info(fields, tenant)}
    ]).

-spec create(map()) -> {ok, map()} | {error, any()}.
create(Tenant) ->
    Now = now_second(),
    Tenant1 = to_tenant(Tenant),
    Tenant2 = Tenant1#tenant{created_at = Now, updated_at = Now},
    trans(fun ?MODULE:do_create/1, [Tenant2]).

-spec read(tenant_id()) -> {ok, map()} | {error, any()}.
read(Id) ->
    trans(fun ?MODULE:do_read/1, [Id]).

-spec update(tenant_id(), map()) -> {ok, map()} | {error, any()}.
update(Id, Changed) ->
    case to_tenant(Changed) of
        #tenant{id = Id} = Tenant -> trans(fun ?MODULE:do_update/1, [Tenant]);
        _ -> {error, invalid_tenant}
    end.

-spec delete(tenant_id()) -> ok.
delete(Id) ->
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

do_update(Tenant = #tenant{id = Id}) ->
    case mnesia:read(?TENANCY, Id, write) of
        [] ->
            mnesia:abort(not_found);
        [#tenant{created_at = CreatedAt}] ->
            NewTenant = Tenant#tenant{created_at = CreatedAt, updated_at = now_second()},
            ok = mnesia:write(?TENANCY, NewTenant, write),
            NewTenant
    end.

do_delete(Id) ->
    mnesia:delete(?TENANCY, Id, write).

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
        <<"quota">> := Quota,
        <<"status">> := Status,
        <<"desc">> := Desc
    } = Tenant,
    #tenant{
        id = Id,
        quota = Quota,
        status = Status,
        desc = Desc
    }.

now_second() ->
    os:system_time(second).
