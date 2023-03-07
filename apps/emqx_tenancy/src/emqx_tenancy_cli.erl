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

-export(['tenant-data'/1, 'tenant-quota'/1]).

%% internal exports
-export([restore/4]).

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

'tenant-data'(["export", "all"]) ->
    export(all, path());
'tenant-data'(["import", File]) ->
    import(File);
'tenant-data'(_) ->
    emqx_ctl:usage([
        {"tenant-data export all", "Export all tenant data"},
        {"tenant-data import <File>", "Import tenant data"}
    ]).

'tenant-quota'(["info", Id]) ->
    case emqx_tenancy_quota:info(list_to_binary(Id)) of
        {ok, Info} ->
            emqx_ctl:print("~p~n", [Info]);
        {error, not_found} ->
            emqx_ctl:print("Not Found~n")
    end;
'tenant-quota'(_) ->
    emqx_ctl:usage([
        {"tenant-quota info <Id>", "Inspect quota/usage information for given tenant Id"}
    ]).

%%--------------------------------------------------------------------
%% internal funcs

export(all, Path) ->
    File = filename:join([Path, filename()]),
    case open_write(File, "1.1.0") of
        {ok, Fd} ->
            try
                emqx_ctl:print("Exporting to ~ts~n", [File]),
                Res = lists:map(
                    fun(Tab) ->
                        dump_table(Fd, Tab)
                    end,
                    [emqx_tenancy, emqx_authn_mnesia, emqx_acl, emqx_retainer_message]
                ),
                ok = commit(Fd),
                ok = close(Fd),
                emqx_ctl:print(
                    "Exported ~w tenants, ~w authn users, ~w authz rules, ~w retained msgs.~n",
                    [lists:nth(1, Res), lists:nth(2, Res), lists:nth(3, Res), lists:nth(4, Res)]
                )
            catch
                Error:Reason:Stk ->
                    emqx_ctl:print("Abort: ~p, ~p~n~p~n", [Error, Reason, Stk]),
                    ok = close(Fd),
                    file:delete(File)
            end;
        {error, Reason} ->
            emqx_ctl:print("Failed to open ~ts error ~p~n", [File, Reason])
    end.

import(File) ->
    case open_read(File) of
        {ok, Fd} ->
            emqx_ctl:print("Importing from ~ts~n", [File]),
            try
                _ = read_import(Fd, fun ?MODULE:restore/4),
                emqx_ctl:print("Try to refresh usage..~n"),
                ok = emqx_tenancy_quota:refresh(),
                emqx_ctl:print("Done~n")
            catch
                Error:Reason:Stk ->
                    emqx_ctl:print("Abort: ~p, ~p~n~p~n", [Error, Reason, Stk]),
                    ok = close(Fd)
            end;
        {error, Reason} ->
            emqx_ctl:print("Failed to open ~ts error: ~0p~n", [File, Reason])
    end.

restore(_Tab, _Ver, 'end', Acc = {Succ, Existed, Failed}) ->
    emqx_ctl:print("  succeed: ~w, existed: ~w, failed: ~w~n", [Succ, Existed, Failed]),
    Acc;
restore(emqx_tenancy, "1.1.0", 'start', undefined) ->
    emqx_ctl:print("Try to import tenant records..~n"),
    {_Succ = 0, _Existed = 0, _Failed = 0};
restore(emqx_tenancy, "1.1.0", Tenant = #tenant{}, {Succ, Existed, Failed}) ->
    case emqx_tenancy:create(Tenant) of
        {ok, _} ->
            {Succ + 1, Existed, Failed};
        {error, already_existed} ->
            {Succ, Existed + 1, Failed};
        {error, _Reason} ->
            {Succ, Existed, Failed + 1}
    end;
restore(emqx_authn_mnesia, "1.1.0", 'start', undefined) ->
    emqx_ctl:print("Try to import authn records..~n"),
    {_Succ = 0, _Existed = 0, _Failed = 0};
restore(emqx_authn_mnesia, "1.1.0", UserInfo, {Succ, Existed, Failed}) ->
    case emqx_authn_mnesia:import_via_raw_record(UserInfo) of
        ok ->
            {Succ + 1, Existed, Failed};
        {error, already_existed} ->
            {Succ, Existed + 1, Failed};
        {error, _Reason} ->
            {Succ, Existed, Failed + 1}
    end;
restore(emqx_acl, "1.1.0", 'start', undefined) ->
    emqx_ctl:print("Try to import authz records..~n"),
    {_Succ = 0, _Existed = 0, _Failed = 0};
restore(emqx_acl, "1.1.0", ACL, {Succ, Existed, Failed}) ->
    case emqx_authz_mnesia:import_via_raw_record(ACL) of
        ok ->
            {Succ + 1, Existed, Failed};
        {error, already_existed} ->
            {Succ, Existed + 1, Failed};
        {error, _Reason} ->
            {Succ, Existed, Failed + 1}
    end;
restore(emqx_retainer_message, "1.1.0", 'start', undefined) ->
    emqx_ctl:print("Try to import retained message records..~n"),
    {_Succ = 0, _Existed = 0, _Failed = 0};
restore(emqx_retainer_message, "1.1.0", Msg, {Succ, Existed, Failed}) ->
    case emqx_retainer_mnesia:import_via_raw_record(Msg) of
        ok ->
            {Succ + 1, Existed, Failed};
        {error, already_existed} ->
            {Succ, Existed + 1, Failed};
        {error, _Reason} ->
            {Succ, Existed, Failed + 1}
    end.

%%--------------------------------------------------------------------
%% utils

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

%%--------------------------------------------------------------------
%% Disklog Interfaces

-spec open_write(file:filename(), string()) -> Fd :: term().
open_write(File, Version) ->
    Opts = [{name, make_ref()}, {repair, false}, {file, File}, {linkto, self()}],
    case disk_log:open(Opts) of
        {ok, Fd} ->
            ok = disk_log:log_terms(Fd, [{version, Version}]),
            {ok, Fd};
        {error, Reason} ->
            {error, Reason}
    end.

-spec dump_table(term(), atom()) -> non_neg_integer().
dump_table(Fd, Tab) ->
    case ets:info(Tab, size) of
        undefined ->
            error({table_not_existed, Tab});
        _ ->
            ok = disk_log:log_terms(Fd, [{Tab, start}]),
            Records = ets:tab2list(Tab),
            ok = disk_log:log_terms(Fd, Records),
            ok = disk_log:log_terms(Fd, [{Tab, 'end'}]),
            length(Records)
    end.

commit(Fd) ->
    ok = disk_log:sync(Fd).

close(Fd) ->
    ok = disk_log:close(Fd).

open_read(File) ->
    Opts = [
        {name, make_ref()},
        {repair, false},
        {file, File},
        {linkto, self()},
        {mode, read_only}
    ],
    disk_log:open(Opts).

read_import(Fd, Callback) ->
    case disk_log:chunk(Fd, start) of
        {Cont, [{version, Version} | Terms]} ->
            St = #{
                fd => Fd,
                continuation => Cont,
                version => Version,
                callback => Callback,
                result => #{}
            },
            do_read_import(undefined, Terms, undefined, St);
        {error, Reason} ->
            {error, Reason};
        eof ->
            {error, bad_file_format}
    end.

do_read_import(Tab, [], Acc, St = #{fd := Fd, continuation := Cont}) ->
    case disk_log:chunk(Fd, Cont) of
        {error, Reason} ->
            error(Reason);
        {Cont1, Terms} ->
            do_read_import(Tab, Terms, Acc, St#{continuation := Cont1});
        eof ->
            maps:get(result, St)
    end;
do_read_import(
    undefined,
    [{Tab, start} | Terms],
    undefined,
    St = #{callback := Callback, version := Version}
) ->
    Acc1 = apply(Callback, [Tab, Version, 'start', undefined]),
    do_read_import(Tab, Terms, Acc1, St);
do_read_import(
    Tab,
    [{Tab, 'end'} | Terms],
    Acc,
    St = #{callback := Callback, version := Version, result := Result}
) ->
    Acc1 = apply(Callback, [Tab, Version, 'end', Acc]),
    St1 = St#{result := Result#{Tab => Acc1}},
    do_read_import(undefined, Terms, undefined, St1);
do_read_import(
    Tab,
    [Rec | Terms],
    Acc,
    St = #{callback := Callback, version := Version}
) ->
    Acc1 = apply(Callback, [Tab, Version, Rec, Acc]),
    do_read_import(Tab, Terms, Acc1, St).
