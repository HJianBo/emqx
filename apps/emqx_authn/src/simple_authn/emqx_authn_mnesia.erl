%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_mnesia).

-include("emqx_authn.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_authentication).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-export([
    refs/0,
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

-export([
    import_users/3,
    add_user/3,
    delete_user/3,
    update_user/4,
    lookup_user/3,
    list_users/3
]).

-export([
    query/4,
    format_user_info/1
]).

%% Internal exports (RPC)
-export([
    do_destroy/1,
    do_add_user/3,
    do_delete_user/3,
    do_update_user/4,
    import/3,
    import_csv/4
]).

-type user_group() :: binary().
-type user_id() :: binary().

-record(user_info, {
    user_id :: {user_group(), emqx_types:tenant_id(), user_id()},
    password_hash :: binary(),
    salt :: binary(),
    is_superuser :: boolean()
}).

-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).

-define(TAB, ?MODULE).
-define(AUTHN_QSCHEMA, [
    %% internal params
    {<<"user_group">>, binary},
    %% internal params
    {<<"tenant">>, binary},
    {<<"like_user_id">>, binary},
    {<<"is_superuser">>, atom}
]).
-define(QUERY_FUN, {?MODULE, query}).

-dialyzer(
    {nowarn_function, [
        qs2ms/1,
        do_destroy/1,
        query/4,
        fuzzy_filter_fun/1,
        run_fuzzy_filter/2
    ]}
).

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

%% @doc Create or replicate tables.
-spec mnesia(boot | copy) -> ok.
mnesia(boot) ->
    ok = mria:create_table(?TAB, [
        %% XXX: Uncomment it on 5.1
        %%{type, ordered_set},
        {rlog_shard, ?AUTH_SHARD},
        {storage, disc_copies},
        {record_name, user_info},
        {attributes, record_info(fields, user_info)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() -> "authn-builtin_db".

roots() -> [?CONF_NS].

fields(?CONF_NS) ->
    [
        {mechanism, emqx_authn_schema:mechanism(password_based)},
        {backend, emqx_authn_schema:backend(built_in_database)},
        {user_id_type, fun user_id_type/1},
        {password_hash_algorithm, fun emqx_authn_password_hashing:type_rw/1}
    ] ++ emqx_authn_schema:common_fields().

desc(?CONF_NS) ->
    ?DESC(?CONF_NS);
desc(_) ->
    undefined.

user_id_type(type) -> hoconsc:enum([clientid, username]);
user_id_type(desc) -> ?DESC(?FUNCTION_NAME);
user_id_type(default) -> <<"username">>;
user_id_type(required) -> true;
user_id_type(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

refs() ->
    [hoconsc:ref(?MODULE, ?CONF_NS)].

create(_AuthenticatorID, Config) ->
    create(Config).

create(
    #{
        user_id_type := Type,
        password_hash_algorithm := Algorithm,
        user_group := UserGroup
    }
) ->
    ok = emqx_authn_password_hashing:init(Algorithm),
    State = #{
        user_group => UserGroup,
        user_id_type => Type,
        password_hash_algorithm => Algorithm
    },
    {ok, State}.

update(Config, _State) ->
    create(Config).

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(
    #{password := Password} = Credential,
    #{
        user_group := UserGroup,
        user_id_type := Type,
        password_hash_algorithm := Algorithm
    }
) ->
    Tenant = maps:get(tenant_id, Credential, ?NO_TENANT),
    UserID = get_user_identity(Credential, Type),
    case mnesia:dirty_read(?TAB, {UserGroup, Tenant, UserID}) of
        [] ->
            ?TRACE_AUTHN_PROVIDER("user_not_found"),
            ignore;
        [#user_info{password_hash = PasswordHash, salt = Salt, is_superuser = IsSuperuser}] ->
            case
                emqx_authn_password_hashing:check_password(
                    Algorithm, Salt, PasswordHash, Password
                )
            of
                true ->
                    {ok, #{is_superuser => IsSuperuser}};
                false ->
                    {error, bad_username_or_password}
            end
    end.

destroy(#{user_group := UserGroup}) ->
    trans(fun ?MODULE:do_destroy/1, [UserGroup]).

do_destroy(UserGroup) ->
    ok = lists:foreach(
        fun(User) ->
            mnesia:delete_object(?TAB, User, write)
        end,
        mnesia:select(?TAB, qs2ms([{user_group, '=:=', UserGroup}]), write)
    ).

import_users(Tenant, {Filename0, FileData}, State) ->
    Filename = to_binary(Filename0),
    case filename:extension(Filename) of
        <<".json">> ->
            import_users_from_json(Tenant, FileData, State);
        <<".csv">> ->
            CSV = csv_data(FileData),
            import_users_from_csv(Tenant, CSV, State);
        <<>> ->
            {error, unknown_file_format};
        Extension ->
            {error, {unsupported_file_format, Extension}}
    end.

add_user(Tenant, UserInfo, State) ->
    trans(fun ?MODULE:do_add_user/3, [Tenant, UserInfo, State]).

do_add_user(
    Tenant,
    #{
        user_id := UserID,
        password := Password
    } = UserInfo,
    #{
        user_group := UserGroup,
        password_hash_algorithm := Algorithm
    }
) ->
    case mnesia:read(?TAB, {UserGroup, Tenant, UserID}, write) of
        [] ->
            {PasswordHash, Salt} = emqx_authn_password_hashing:hash(Algorithm, Password),
            IsSuperuser = maps:get(is_superuser, UserInfo, false),
            insert_user(UserGroup, Tenant, UserID, PasswordHash, Salt, IsSuperuser),
            {ok, #{user_id => UserID, is_superuser => IsSuperuser}};
        [_] ->
            {error, already_exist}
    end.

delete_user(Tenant, UserID, State) ->
    trans(fun ?MODULE:do_delete_user/3, [Tenant, UserID, State]).

do_delete_user(Tenant, UserID, #{user_group := UserGroup}) ->
    case mnesia:read(?TAB, {UserGroup, Tenant, UserID}, write) of
        [] ->
            {error, not_found};
        [_] ->
            mnesia:delete(?TAB, {UserGroup, Tenant, UserID}, write)
    end.

update_user(Tenant, UserID, UserInfo, State) ->
    trans(fun ?MODULE:do_update_user/4, [Tenant, UserID, UserInfo, State]).

do_update_user(
    Tenant,
    UserID,
    UserInfo,
    #{
        user_group := UserGroup,
        password_hash_algorithm := Algorithm
    }
) ->
    case mnesia:read(?TAB, {UserGroup, Tenant, UserID}, write) of
        [] ->
            {error, not_found};
        [
            #user_info{
                password_hash = PasswordHash,
                salt = Salt,
                is_superuser = IsSuperuser
            }
        ] ->
            NSuperuser = maps:get(is_superuser, UserInfo, IsSuperuser),
            {NPasswordHash, NSalt} =
                case UserInfo of
                    #{password := Password} ->
                        emqx_authn_password_hashing:hash(
                            Algorithm, Password
                        );
                    #{} ->
                        {PasswordHash, Salt}
                end,
            insert_user(UserGroup, Tenant, UserID, NPasswordHash, NSalt, NSuperuser),
            {ok, #{user_id => UserID, is_superuser => NSuperuser}}
    end.

lookup_user(Tenant, UserID, #{user_group := UserGroup}) ->
    case mnesia:dirty_read(?TAB, {UserGroup, Tenant, UserID}) of
        [UserInfo] ->
            {ok, format_user_info(UserInfo)};
        [] ->
            {error, not_found}
    end.

list_users(Tenant, QueryString, #{user_group := UserGroup}) ->
    NQueryString = QueryString#{<<"user_group">> => UserGroup, <<"tenant">> => Tenant},
    emqx_mgmt_api:node_query(node(), NQueryString, ?TAB, ?AUTHN_QSCHEMA, ?QUERY_FUN).

%%--------------------------------------------------------------------
%% Query Functions

query(Tab, {QString, []}, Continuation, Limit) ->
    Ms = qs2ms(QString),
    emqx_mgmt_api:select_table_with_count(
        Tab,
        Ms,
        Continuation,
        Limit,
        fun format_user_info/1
    );
query(Tab, {QString, FuzzyQString}, Continuation, Limit) ->
    Ms = qs2ms(QString),
    FuzzyFilterFun = fuzzy_filter_fun(FuzzyQString),
    emqx_mgmt_api:select_table_with_count(
        Tab,
        {Ms, FuzzyFilterFun},
        Continuation,
        Limit,
        fun format_user_info/1
    ).

%%--------------------------------------------------------------------
%% Match funcs

%% Fuzzy username funcs
fuzzy_filter_fun(Fuzzy) ->
    fun(MsRaws) when is_list(MsRaws) ->
        lists:filter(
            fun(E) -> run_fuzzy_filter(E, Fuzzy) end,
            MsRaws
        )
    end.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(
    E = #user_info{user_id = {_, _, UserID}},
    [{user_id, like, UsernameSubStr} | Fuzzy]
) ->
    binary:match(UserID, UsernameSubStr) /= nomatch andalso run_fuzzy_filter(E, Fuzzy).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

%% Example: data/user-credentials.json
import_users_from_json(Tenant, Bin, #{user_group := UserGroup}) ->
    case emqx_json:safe_decode(Bin, [return_maps]) of
        {ok, List} ->
            trans(fun ?MODULE:import/3, [UserGroup, Tenant, List]);
        {error, Reason} ->
            {error, Reason}
    end.

%% Example: data/user-credentials.csv
import_users_from_csv(Tenant, CSV, #{user_group := UserGroup}) ->
    case get_csv_header(CSV) of
        {ok, Seq, NewCSV} ->
            trans(fun ?MODULE:import_csv/4, [UserGroup, Tenant, NewCSV, Seq]);
        {error, Reason} ->
            {error, Reason}
    end.

import(_UserGroup, _Tenant, []) ->
    ok;
import(UserGroup, Tenant, [
    #{
        <<"user_id">> := UserID,
        <<"password_hash">> := PasswordHash
    } = UserInfo
    | More
]) when
    is_binary(UserID) andalso is_binary(PasswordHash)
->
    Salt = maps:get(<<"salt">>, UserInfo, <<>>),
    IsSuperuser = maps:get(<<"is_superuser">>, UserInfo, false),
    insert_user(UserGroup, Tenant, UserID, PasswordHash, Salt, IsSuperuser),
    import(UserGroup, Tenant, More);
import(_UserGroup, _Tenant, [_ | _More]) ->
    {error, bad_format}.

%% Importing 5w users needs 1.7 seconds
import_csv(UserGroup, Tenant, CSV, Seq) ->
    case csv_read_line(CSV) of
        {ok, Line, NewCSV} ->
            Fields = binary:split(Line, [<<",">>, <<" ">>, <<"\n">>], [global, trim_all]),
            case get_user_info_by_seq(Fields, Seq) of
                {ok,
                    #{
                        user_id := UserID,
                        password_hash := PasswordHash
                    } = UserInfo} ->
                    Salt = maps:get(salt, UserInfo, <<>>),
                    IsSuperuser = maps:get(is_superuser, UserInfo, false),
                    insert_user(UserGroup, Tenant, UserID, PasswordHash, Salt, IsSuperuser),
                    import_csv(UserGroup, Tenant, NewCSV, Seq);
                {error, Reason} ->
                    {error, Reason}
            end;
        eof ->
            ok
    end.

get_csv_header(CSV) ->
    case csv_read_line(CSV) of
        {ok, Line, NewCSV} ->
            Seq = binary:split(Line, [<<",">>, <<" ">>, <<"\n">>], [global, trim_all]),
            {ok, Seq, NewCSV};
        eof ->
            {error, empty_file}
    end.

get_user_info_by_seq(Fields, Seq) ->
    get_user_info_by_seq(Fields, Seq, #{}).

get_user_info_by_seq([], [], #{user_id := _, password_hash := _} = Acc) ->
    {ok, Acc};
get_user_info_by_seq(_, [], _) ->
    {error, bad_format};
get_user_info_by_seq([Tenant | More1], [<<"tenant_id">> | More2], Acc) ->
    get_user_info_by_seq(More1, More2, Acc#{tenant_id => Tenant});
get_user_info_by_seq([UserID | More1], [<<"user_id">> | More2], Acc) ->
    get_user_info_by_seq(More1, More2, Acc#{user_id => UserID});
get_user_info_by_seq([PasswordHash | More1], [<<"password_hash">> | More2], Acc) ->
    get_user_info_by_seq(More1, More2, Acc#{password_hash => PasswordHash});
get_user_info_by_seq([Salt | More1], [<<"salt">> | More2], Acc) ->
    get_user_info_by_seq(More1, More2, Acc#{salt => Salt});
get_user_info_by_seq([<<"true">> | More1], [<<"is_superuser">> | More2], Acc) ->
    get_user_info_by_seq(More1, More2, Acc#{is_superuser => true});
get_user_info_by_seq([<<"false">> | More1], [<<"is_superuser">> | More2], Acc) ->
    get_user_info_by_seq(More1, More2, Acc#{is_superuser => false});
get_user_info_by_seq(_, _, _) ->
    {error, bad_format}.

insert_user(UserGroup, Tenant, UserID, PasswordHash, Salt, IsSuperuser) ->
    UserInfo = #user_info{
        user_id = {UserGroup, Tenant, UserID},
        password_hash = PasswordHash,
        salt = Salt,
        is_superuser = IsSuperuser
    },
    mnesia:write(?TAB, UserInfo, write).

%% TODO: Support other type
get_user_identity(#{username := Username}, username) ->
    Username;
get_user_identity(#{clientid := ClientID}, clientid) ->
    ClientID;
get_user_identity(_, Type) ->
    {error, {bad_user_identity_type, Type}}.

trans(Fun, Args) ->
    case mria:transaction(?AUTH_SHARD, Fun, Args) of
        {atomic, Res} -> Res;
        {aborted, Reason} -> {error, Reason}
    end.

to_binary(B) when is_binary(B) ->
    B;
to_binary(L) when is_list(L) ->
    iolist_to_binary(L).

format_user_info(#user_info{user_id = {_, _, UserID}, is_superuser = IsSuperuser}) ->
    #{user_id => UserID, is_superuser => IsSuperuser}.

qs2ms(Qs) when is_list(Qs) ->
    [{Mh0, Conds0, Return}] = ets:fun2ms(fun(User = #user_info{}) -> User end),
    %% assert
    true = lists:all(fun({_, '=:=', _}) -> true end, Qs),
    %% assemble match spec head and conditions
    QsMap = maps:from_list(
        lists:filtermap(
            fun
                ({K, _, V}) ->
                    {true, {K, V}};
                (_) ->
                    false
            end,
            Qs
        )
    ),
    {Mh, Conds} =
        case QsMap of
            %% query `undefined` tenant means all tenants
            #{user_group := Group, tenant_id := Tenant} when Tenant =/= undefined ->
                {Mh0#user_info{user_id = {'$1', '$2', '_'}}, [
                    {'=:=', '$1', Group},
                    {'=:=', '$2', Tenant}
                    | Conds0
                ]};
            #{user_group := Group} ->
                {Mh0#user_info{user_id = {'$1', '_', '_'}}, [{'=:=', '$1', Group}]}
        end,
    {Mh1, Conds1} =
        case QsMap of
            #{is_superuser := IsSup} ->
                {Mh#user_info{is_superuser = '$3'}, [{'=:=', '$3', IsSup} | Conds]};
            _ ->
                {Mh, Conds}
        end,
    %% TODO: compatible the old user id schema?
    [{Mh1, Conds1, Return}].

csv_data(Data) ->
    Lines = binary:split(Data, [<<"\r">>, <<"\n">>], [global, trim_all]),
    {csv_data, Lines}.

csv_read_line({csv_data, [Line | Lines]}) ->
    {ok, Line, {csv_data, Lines}};
csv_read_line({csv_data, []}) ->
    eof.
