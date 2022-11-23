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

-module(emqx_authz_mnesia).

-include_lib("emqx/include/emqx.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-include("emqx_authz.hrl").

-define(ACL_SHARDED, emqx_acl_sharded).

%% To save some space, use an integer for label, 0 for 'all', {1, Username} and {2, ClientId}.
-define(ACL_TABLE_ALL, 0).
-define(ACL_TABLE_USERNAME, 1).
-define(ACL_TABLE_CLIENTID, 2).
-define(NAME_TO_LABLE(N),
    case N of
        all -> 0;
        username -> 1;
        clientid -> 2
    end
).

-type all() :: {all, emqx_types:tenant_id()}.
-type username() :: {username, emqx_types:tenant_id(), binary()}.
-type clientid() :: {clientid, emqx_types:tenant_id(), binary()}.
-type who() :: username() | clientid() | all().

-type rule() :: {emqx_authz_rule:permission(), emqx_authz_rule:action(), emqx_topic:topic()}.
-type rules() :: [rule()].

-record(emqx_acl, {
    who ::
        {?ACL_TABLE_ALL, emqx_types:tenant_id()}
        | {?ACL_TABLE_USERNAME, emqx_types:tenant_id(), binary()}
        | {?ACL_TABLE_CLIENTID, emqx_types:tenant_id(), binary()},
    rules :: rules()
}).

-behaviour(emqx_authz).

%% AuthZ Callbacks
-export([
    description/0,
    create/1,
    update/1,
    destroy/1,
    authorize/4
]).

%% Management API
-export([
    mnesia/1,
    init_tables/0,
    store_rules/2,
    purge_rules/0,
    lookup_rules/1,
    delete_rules/1,
    list_username_rules/1,
    list_clientid_rules/1,
    list_all_rules/1,
    record_count/0,
    do_tenant_deleted/1
]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-boot_mnesia({mnesia, [boot]}).

-spec mnesia(boot | copy) -> ok.
mnesia(boot) ->
    ok = mria:create_table(?ACL_TABLE, [
        {type, ordered_set},
        {rlog_shard, ?ACL_SHARDED},
        {storage, disc_copies},
        {attributes, record_info(fields, ?ACL_TABLE)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ]).

%%--------------------------------------------------------------------
%% emqx_authz callbacks
%%--------------------------------------------------------------------

description() ->
    "AuthZ with Mnesia".

create(Source) -> Source.

update(Source) -> Source.

destroy(_Source) -> ok.

authorize(
    #{
        username := Username,
        clientid := ClientId
    } = Client,
    PubSub,
    Topic,
    #{type := built_in_database}
) ->
    Tenant = maps:get(tenant_id, Client, ?NO_TENANT),
    Rules =
        case mnesia:dirty_read(?ACL_TABLE, {?ACL_TABLE_CLIENTID, Tenant, ClientId}) of
            [] -> [];
            [#emqx_acl{rules = Rules0}] when is_list(Rules0) -> Rules0
        end ++
            case mnesia:dirty_read(?ACL_TABLE, {?ACL_TABLE_USERNAME, Tenant, Username}) of
                [] -> [];
                [#emqx_acl{rules = Rules1}] when is_list(Rules1) -> Rules1
            end ++
            case mnesia:dirty_read(?ACL_TABLE, {?ACL_TABLE_ALL, Tenant}) of
                [] -> [];
                [#emqx_acl{rules = Rules2}] when is_list(Rules2) -> Rules2
            end,
    do_authorize(Client, PubSub, Topic, Rules).

%%--------------------------------------------------------------------
%% Management API
%%--------------------------------------------------------------------

%% Init
-spec init_tables() -> ok.
init_tables() ->
    ok = mria_rlog:wait_for_shards([?ACL_SHARDED], infinity).

%% @doc Update authz rules
-spec store_rules(who(), rules()) -> ok.
store_rules({username, Tenant, Username}, Rules) ->
    Record = #emqx_acl{
        who = {?ACL_TABLE_USERNAME, Tenant, Username}, rules = normalize_rules(Rules)
    },
    mria:dirty_write(Record);
store_rules({clientid, Tenant, Clientid}, Rules) ->
    Record = #emqx_acl{
        who = {?ACL_TABLE_CLIENTID, Tenant, Clientid}, rules = normalize_rules(Rules)
    },
    mria:dirty_write(Record);
store_rules({all, Tenant}, Rules) ->
    Record = #emqx_acl{who = {?ACL_TABLE_ALL, Tenant}, rules = normalize_rules(Rules)},
    mria:dirty_write(Record).

%% @doc Clean all authz rules for (username & clientid & all)
-spec purge_rules() -> ok.
purge_rules() ->
    ok = lists:foreach(
        fun(Key) ->
            ok = mria:dirty_delete(?ACL_TABLE, Key)
        end,
        mnesia:dirty_all_keys(?ACL_TABLE)
    ).

%% @doc Get one record
-spec lookup_rules(who()) -> {ok, rules()} | not_found.
lookup_rules({username, Tenant, Username}) ->
    do_lookup_rules({?ACL_TABLE_USERNAME, Tenant, Username});
lookup_rules({clientid, Tenant, Clientid}) ->
    do_lookup_rules({?ACL_TABLE_CLIENTID, Tenant, Clientid});
lookup_rules({all, Tenant}) ->
    do_lookup_rules({?ACL_TABLE_ALL, Tenant}).

%% @doc Delete one record
-spec delete_rules(who()) -> ok.
delete_rules({username, Tenant, Username}) ->
    mria:dirty_delete(?ACL_TABLE, {?ACL_TABLE_USERNAME, Tenant, Username});
delete_rules({clientid, Tenant, Clientid}) ->
    mria:dirty_delete(?ACL_TABLE, {?ACL_TABLE_CLIENTID, Tenant, Clientid});
delete_rules({all, Tenant}) ->
    mria:dirty_delete(?ACL_TABLE, {?ACL_TABLE_ALL, Tenant}).

-spec list_username_rules(emqx_types:tenant_id() | undefined) -> ets:match_spec().
list_username_rules(undefined) ->
    ets:fun2ms(
        fun(#emqx_acl{who = {?ACL_TABLE_USERNAME, '_', Username}, rules = Rules}) ->
            [{username, Username}, {rules, Rules}]
        end
    );
list_username_rules(Tenant0) ->
    ets:fun2ms(
        fun(#emqx_acl{who = {?ACL_TABLE_USERNAME, Tenant, Username}, rules = Rules}) when
            Tenant =:= Tenant0
        ->
            [{username, Username}, {rules, Rules}]
        end
    ).

-spec list_clientid_rules(emqx_types:tenant_id() | undefined) -> ets:match_spec().
list_clientid_rules(undefined) ->
    ets:fun2ms(
        fun(#emqx_acl{who = {?ACL_TABLE_CLIENTID, '_', Clientid}, rules = Rules}) ->
            [{clientid, Clientid}, {rules, Rules}]
        end
    );
list_clientid_rules(Tenant0) ->
    ets:fun2ms(
        fun(#emqx_acl{who = {?ACL_TABLE_CLIENTID, Tenant, Clientid}, rules = Rules}) when
            Tenant =:= Tenant0
        ->
            [{clientid, Clientid}, {rules, Rules}]
        end
    ).

-spec list_all_rules(emqx_types:tenant_id() | undefined) -> ets:match_spec().
list_all_rules(undefined) ->
    ets:fun2ms(
        fun(#emqx_acl{who = {?ACL_TABLE_ALL, '_'}, rules = Rules}) ->
            Rules
        end
    );
list_all_rules(Tenant0) ->
    ets:fun2ms(
        fun(#emqx_acl{who = {?ACL_TABLE_ALL, Tenant}, rules = Rules}) when
            Tenant =:= Tenant0
        ->
            Rules
        end
    ).

-spec record_count() -> non_neg_integer().
record_count() ->
    mnesia:table_info(?ACL_TABLE, size).

do_tenant_deleted(Tenant) ->
    Ms = ets:fun2ms(fun(#emqx_acl{who = Who}) when element(2, Who) =:= Tenant -> Who end),
    All = mnesia:select(?ACL_TABLE, Ms, read),
    lists:foreach(fun(K) -> mnesia:delete(?ACL_TABLE, K, write) end, All).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

normalize_rules(Rules) ->
    lists:map(fun normalize_rule/1, Rules).

normalize_rule({Permission, Action, Topic}) ->
    {normalize_permission(Permission), normalize_action(Action), normalize_topic(Topic)};
normalize_rule(Rule) ->
    error({invalid_rule, Rule}).

normalize_topic(Topic) when is_list(Topic) -> list_to_binary(Topic);
normalize_topic(Topic) when is_binary(Topic) -> Topic;
normalize_topic(Topic) -> error({invalid_rule_topic, Topic}).

normalize_action(publish) -> publish;
normalize_action(subscribe) -> subscribe;
normalize_action(all) -> all;
normalize_action(Action) -> error({invalid_rule_action, Action}).

normalize_permission(allow) -> allow;
normalize_permission(deny) -> deny;
normalize_permission(Permission) -> error({invalid_rule_permission, Permission}).

do_lookup_rules(Key) ->
    case mnesia:dirty_read(?ACL_TABLE, Key) of
        [#emqx_acl{rules = Rules}] -> {ok, Rules};
        [] -> not_found
    end.

do_authorize(_Client, _PubSub, _Topic, []) ->
    nomatch;
do_authorize(Client, PubSub, Topic, [{Permission, Action, TopicFilter} | Tail]) ->
    Rule = emqx_authz_rule:compile({Permission, all, Action, [TopicFilter]}),
    case emqx_authz_rule:match(Client, PubSub, Topic, Rule) of
        {matched, Permission} -> {matched, Permission};
        nomatch -> do_authorize(Client, PubSub, Topic, Tail)
    end.
