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

-module(emqx_tenancy_cli).

-include("emqx_tenancy.hrl").

-export([load/0, unload/0]).

-export(['tenants-data'/1]).

%%--------------------------------------------------------------------
%% load

load() ->
    Cmds = [Fun || {Fun, _} <- ?MODULE:module_info(exports), is_cmd(Fun)],
    lists:foreach(fun(Cmd) -> emqx_ctl:register_command(Cmd, {?MODULE, Cmd}, []) end, Cmds).

-spec unload() -> ok.
unload() ->
    Cmds = [Fun || {Fun, _} <- ?MODULE:module_info(exports), is_cmd(Fun)],
    lists:foreach(fun(Cmd) -> emqx_ctl:unregister_command(Cmd) end, Cmds).

is_cmd(Fun) ->
    not lists:member(Fun, [init, load, unload, module_info]).

%%--------------------------------------------------------------------
%% commands

'tenants-data'(["export", "all"]) ->
    export(all, path());
'tenants-data'(["import", File]) ->
    import(File);
'tenants-data'(_) ->
    emqx_ctl:usage([
        {"tenants-data export all", "Export all tenants data"},
        {"tenants-data import <File>", "Import tenants data"}
    ]).

%%--------------------------------------------------------------------
%% internal funcs

export(all, Path) ->
    File = filename:join([Path, filename()]),
    case file:open(File, [write]) of
        {ok, Fd} ->
            try
                emqx_ctl:print("Exporting to ~ts~n", [File]),
                Tenants = dump_tenants(),
                AuthnUsers = dump_authn_users(),
                AuthzRules = dump_authz_rules(),
                Term = #{
                    version => "1.1.0",
                    tenants => Tenants,
                    authn_users => AuthnUsers,
                    authz_rules => AuthzRules
                },
                file:write(Fd, term_to_binary(Term)),
                emqx_ctl:print(
                    "Exported ~w tenants, ~w authn records, ~w authz records.~n",
                    [length(Tenants), length(AuthnUsers), length(AuthzRules)]
                )
            catch
                _:_ ->
                    ok = file:close(Fd)
            end;
        {error, Reason} ->
            emqx_ctl:print("Failed to open ~ts error ~p~n", [File, Reason])
    end.

import(File) ->
    emqx_ctl:print("Importing from ~ts~n", [File]),
    case file:read_file(File) of
        {ok, Binary} ->
            Term = binary_to_term(Binary),
            Version = maps:get(version, Term),
            ok = restore_tenants(Version, maps:get(tenants, Term)),
            ok = restore_authn_users(Version, maps:get(authn_users, Term)),
            ok = restore_authz_rules(Version, maps:get(authz_rules, Term)),
            emqx_ctl:print("Done~n");
        {error, Reason} ->
            emqx_ctl:print("Failed to open ~ts error: ~0p~n", [File, Reason])
    end.

dump_tenants() ->
    ets:tab2list(emqx_tenancy).

restore_tenants("1.1.0", Rows) ->
    emqx_ctl:print("Try to import ~w tenant records..~n", [length(Rows)]),
    {Succ, Existed, Failed} =
        lists:foldl(
            fun(
                Tenant = #tenant{},
                {A, B, C}
            ) ->
                case emqx_tenancy:create(Tenant) of
                    {ok, _} ->
                        {A + 1, B, C};
                    {error, already_existed} ->
                        {A, B + 1, C};
                    {error, _Reason} ->
                        {A, B, C + 1}
                end
            end,
            {0, 0, 0},
            Rows
        ),
    emqx_ctl:print("  succeed: ~w, existed: ~w, failed: ~w~n", [Succ, Existed, Failed]).

dump_authn_users() ->
    emqx_authn_mnesia:dump_all_users().

restore_authn_users("1.1.0", Rows) ->
    emqx_ctl:print("Try to import ~w authn records..~n", [length(Rows)]),
    {Succ, Existed, Failed} =
        lists:foldl(
            fun(UserInfo, {A, B, C}) ->
                case emqx_authn_mnesia:import_via_raw_record(UserInfo) of
                    ok ->
                        {A + 1, B, C};
                    {error, already_existed} ->
                        {A, B + 1, C};
                    {error, _Reason} ->
                        {A, B, C + 1}
                end
            end,
            {0, 0, 0},
            Rows
        ),
    emqx_ctl:print("  succeed: ~w, existed: ~w, failed: ~w~n", [Succ, Existed, Failed]).

dump_authz_rules() ->
    emqx_authz_mnesia:dump_all_rules().

restore_authz_rules("1.1.0", Rows) ->
    emqx_ctl:print("Try to import ~w authz records..~n", [length(Rows)]),
    {Succ, Existed, Failed} =
        lists:foldl(
            fun(ACL, {A, B, C}) ->
                case emqx_authz_mnesia:import_via_raw_record(ACL) of
                    ok ->
                        {A + 1, B, C};
                    {error, already_existed} ->
                        {A, B + 1, C};
                    {error, _Reason} ->
                        {A, B, C + 1}
                end
            end,
            {0, 0, 0},
            Rows
        ),
    emqx_ctl:print("  succeed: ~w, existed: ~w, failed: ~w~n", [Succ, Existed, Failed]).

path() ->
    Path = filename:join([emqx:data_dir(), "exports"]),
    case file:make_dir(Path) of
        ok -> Path;
        {error, eexist} -> Path;
        {error, Reason} -> error(Reason)
    end.

filename() ->
    {Y, M, D} = date(),
    {Hh, Mm, Ss} = time(),
    Id = emqx_misc:gen_id(),
    lists:flatten(
        io_lib:format(
            "backup-~4w~2.10.0w~2.10.0w"
            "~2.10.0w~2.10.0w~2.10.0w-~s",
            [Y, M, D, Hh, Mm, Ss, Id]
        )
    ).
