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

-module(emqx_clientid).

-include("emqx.hrl").

-export([with_tenant/2, without_tenant/1]).

-export([update_clientid/2, is_undefined_clientid/1]).

-type clientid() :: binary() | atom().

-type grouped_clientid() :: {emqx_types:tenant_id(), clientid()} | clientid().

-export_type([clientid/0, grouped_clientid/0]).

-define(IS_NORMAL_ID(I), (is_atom(I) orelse is_binary(I))).

-define(IS_TENANT(I), is_binary(I)).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec without_tenant(grouped_clientid()) -> clientid().
without_tenant({_, ClientId}) ->
    ClientId;
without_tenant(ClientId) when ?IS_NORMAL_ID(ClientId) ->
    ClientId;
without_tenant(ClientId) ->
    error(badarg, [ClientId]).

-spec with_tenant(emqx_types:tenant_id(), grouped_clientid()) -> grouped_clientid().
with_tenant(?NO_TENANT, ClientId) when ?IS_NORMAL_ID(ClientId) ->
    ClientId;
with_tenant(?NO_TENANT, {_, ClientId}) ->
    ClientId;
with_tenant(Tenant, _GroupedClientId = {_, ClientId}) when
    ?IS_TENANT(Tenant), ?IS_NORMAL_ID(ClientId)
->
    {Tenant, ClientId};
with_tenant(Tenant, ClientId) when
    ?IS_TENANT(Tenant), ?IS_NORMAL_ID(ClientId)
->
    {Tenant, ClientId};
with_tenant(Tenant, ClientId) ->
    error(badarg, [Tenant, ClientId]).

-spec update_clientid(emqx_types:tenant_id(), grouped_clientid()) -> grouped_clientid().
update_clientid(Id, _GroupedClientId = {Tenant, _}) ->
    {Tenant, Id};
update_clientid(Id, ClientId) when ?IS_NORMAL_ID(ClientId) ->
    Id;
update_clientid(Id, ClientId) ->
    error(badarg, [Id, ClientId]).

-spec is_undefined_clientid(grouped_clientid()) -> boolean().
is_undefined_clientid(undefined) -> true;
is_undefined_clientid({_, undefined}) -> true;
is_undefined_clientid({_, _}) -> false;
is_undefined_clientid(I) when ?IS_NORMAL_ID(I) -> false;
is_undefined_clientid(V) -> error(badarg, [V]).

%%--------------------------------------------------------------------
%% eunits
%%--------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

with_tenant_test() ->
    {<<"t">>, <<"id">>} = with_tenant(<<"t">>, <<"id">>),
    {<<"t2">>, <<"id">>} = with_tenant(<<"t2">>, {<<"t">>, <<"id">>}),
    <<"id">> = with_tenant(?NO_TENANT, {<<"t">>, <<"id">>}),
    <<"id">> = with_tenant(?NO_TENANT, <<"id">>),
    undefined = with_tenant(?NO_TENANT, undefined),

    ?assertError(badarg, with_tenant(?NO_TENANT, 0)),
    ?assertError(badarg, with_tenant(?NO_TENANT, "")),
    ?assertError(badarg, with_tenant("a", <<"id">>)),
    ?assertError(badarg, with_tenant(123, <<"id">>)),
    ?assertError(badarg, with_tenant(abc, <<"id">>)).

without_tenant_test() ->
    <<"id">> = without_tenant({<<"t">>, <<"id">>}),
    <<"id">> = without_tenant(<<"id">>),
    atom_client_id = without_tenant(atom_client_id),
    ?assertError(badarg, without_tenant("")).

update_clientid_test() ->
    {<<"t">>, <<"id2">>} = update_clientid(<<"id2">>, {<<"t">>, <<"id1">>}),
    <<"id2">> = update_clientid(<<"id2">>, <<"id1">>),
    ?assertError(badarg, update_clientid(<<"id2">>, "bad_grouped_id")).

undefined_clientid_test() ->
    true = is_undefined_clientid(undefined),
    true = is_undefined_clientid(with_tenant(<<"t">>, undefined)),
    false = is_undefined_clientid(with_tenant(<<"t">>, <<"t">>)),
    false = is_undefined_clientid(with_tenant(?NO_TENANT, <<"t">>)),
    false = is_undefined_clientid(with_tenant(?NO_TENANT, atom_clientid)),
    ?assertError(badarg, is_undefined_clientid("bad_grouped_id")).

-endif.
