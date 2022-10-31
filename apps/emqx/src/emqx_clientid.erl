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

-export([comp/2, uncomp/1, parse/1]).

-export([update_tenant/2, update_clientid/2, is_undefined_clientid/1]).

-type clientid() :: binary() | atom().

-type grouped_clientid() :: {emqx_types:tenant(), clientid()} | clientid().

-export_type([clientid/0, grouped_clientid/0]).

-define(IS_NORMAL_ID(I), (is_atom(I) orelse is_binary(I))).

-define(IS_TENANT(I), is_binary(I)).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec comp(emqx_types:tenant(), clientid()) -> grouped_clientid().
comp(Tenant, ClientId) when ?IS_TENANT(Tenant), ?IS_NORMAL_ID(ClientId) ->
    {Tenant, ClientId};
comp(undefined, ClientId) when ?IS_NORMAL_ID(ClientId) ->
    ClientId;
comp(_, _) ->
    error(badarg).

-spec uncomp(grouped_clientid()) -> clientid().
uncomp({_, ClientId}) ->
    ClientId;
uncomp(ClientId) when ?IS_NORMAL_ID(ClientId) ->
    ClientId;
uncomp(_) ->
    error(badarg).

-spec parse(grouped_clientid()) -> {emqx_types:tenant(), clientid()}.
parse({Tenant, ClientId}) ->
    {Tenant, ClientId};
parse(ClientId) when ?IS_NORMAL_ID(ClientId) ->
    {undefined, ClientId};
parse(_) ->
    error(badarg).

-spec update_tenant(emqx_types:tenant(), grouped_clientid()) -> grouped_clientid().
update_tenant(undefined, ClientId) when ?IS_NORMAL_ID(ClientId) ->
    ClientId;
update_tenant(undefined, {_, ClientId}) ->
    ClientId;
update_tenant(Tenant, _GroupedClientId = {_, ClientId}) ->
    {Tenant, ClientId};
update_tenant(Tenant, ClientId) when ?IS_NORMAL_ID(ClientId) ->
    {Tenant, ClientId};
update_tenant(_, _) ->
    error(badarg).

-spec update_clientid(emqx_types:tenant(), grouped_clientid()) -> grouped_clientid().
update_clientid(Id, _GroupedClientId = {Tenant, _}) ->
    {Tenant, Id};
update_clientid(Id, ClientId) when ?IS_NORMAL_ID(ClientId) ->
    Id;
update_clientid(_, _) ->
    error(badarg).

-spec is_undefined_clientid(grouped_clientid()) -> boolean().
is_undefined_clientid(undefined) -> true;
is_undefined_clientid({_, undefined}) -> true;
is_undefined_clientid({_, _}) -> false;
is_undefined_clientid(I) when is_binary(I) -> false;
is_undefined_clientid(_) -> error(badarg).

%%--------------------------------------------------------------------
%% eunits
%%--------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

comp_uncomp_test() ->
    ?assertEqual(
        {<<"test_tenant">>, <<"client1">>},
        comp(<<"test_tenant">>, <<"client1">>)
    ).

-endif.
