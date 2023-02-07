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

-module(emqx_tenancy_limiter_storage).

-behaviour(gen_server).

-include("emqx_tenancy.hrl").

-define(TAB, ?MODULE).

-define(KEY(Node, Id, Type), {Node, Id, Type}).

%% APIs
-export([start_link/0]).

-export([insert/1, lookup/3]).

%% Internal exports
-export([do_insert/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Increase counter in batch
-spec insert(#{tenant_id() => #{limiter_type() => pos_integer()}}) -> ok.
insert(Batch) ->
    Node = node(),
    Objects = maps:fold(
        fun(TenantId, TypedObatained, Acc0) ->
            maps:fold(
                fun(Type, Val, Acc1) ->
                    [{?KEY(Node, TenantId, Type), Val} | Acc1]
                end,
                Acc0,
                TypedObatained
            )
        end,
        [],
        Batch
    ),
    do_insert(Objects),
    lists:foreach(
        fun(RemoteNode) ->
            async_insert(RemoteNode, Objects)
        end,
        nodes()
    ).

do_insert(Objects) ->
    try
        ets:insert(?TAB, Objects)
    catch
        error:badarg -> ok
    end.

async_insert(Node, Objects) ->
    emqx_rpc:cast(Node, ?MODULE, do_insert, [Objects]).

-spec lookup(node(), tenant_id(), limiter_type()) -> integer().
lookup(Node, TenantId, Type) ->
    case ets:lookup(?TAB, ?KEY(Node, TenantId, Type)) of
        [] ->
            undefined;
        [{_, Obtained}] ->
            Obtained
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    ok = emqx_tables:new(?TAB, [public, {write_concurrency, true}]),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------
