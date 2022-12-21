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

-module(emqx_tenancy_stats).

-include("emqx_tenancy.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-behaviour(gen_server).

-boot_mnesia({mnesia, [boot]}).

-export([mnesia/1]).
-export([start_link/0]).

-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([
    on_message_dropped/3,
    on_delivery_dropped/3,
    on_message_publish/1,
    on_message_delivered/2,
    on_client_check_authz_complete/5,
    on_client_check_authn_complete/2
]).

-export([
    update_stats/2,
    update_rate_stats/1,
    sample/1
]).

-define(SAMPLE, emqx_tenancy_sample).
-define(STATS, ?MODULE).

%% TODO collect those by hooks
-define(INIT, #{
    sessions => 0,
    connections => 0,
    topics => 0,
    subscriptions => 0,
    subscriptions_shared => 0,
    msg_retained => 0
}).

-record(stats, {
    tenant_id = <<>>,
    msg_received = 0,
    msg_sent = 0,
    byte_received = 0,
    byte_sent = 0,
    last_msg_received = {0, 0},
    last_msg_sent = {0, 0},
    last_byte_received = {0, 0},
    last_byte_sent = {0, 0},
    msg_dropped = 0,
    last_msg_dropped = {0, 0},
    msg_dropped_queue_full = 0,
    msg_dropped_expired = 0,
    msg_no_subscribers = 0,
    acl_failed = 0,
    auth_failed = 0,
    rule_action_failed = 0,
    rule_sql_failed = 0,
    resource_failed = 0
}).

-define(DB, tenancy_stats).

mnesia(boot) ->
    ok = mria:create_table(?DB, [
        {type, set},
        {local_content, true},
        {storage, disc_copies},
        {record_name, stats},
        {attributes, record_info(fields, stats)}
    ]).

-define(HP_TENANCY, 1).

on_message_publish(#message{flags = #{sys := true}}) ->
    ok;
on_message_publish(#message{from = {Tenant, _}}) ->
    update_counter(Tenant, [{#stats.msg_received, 1}], #stats{tenant_id = Tenant, msg_received = 1});
on_message_publish(_) ->
    ok.

on_message_delivered(#message{flags = #{sys := true}}, _) ->
    ok;
on_message_delivered(_, #message{from = {Tenant, _}}) ->
    update_counter(Tenant, [{#stats.msg_sent, 1}], #stats{tenant_id = Tenant, msg_sent = 1});
on_message_delivered(_, _) ->
    ok.

on_message_dropped(#message{flags = #{sys := true}}, _, _) ->
    ok;
on_message_dropped(#message{from = {Tenant, _}}, _, no_subscribers) ->
    UpdateOpt = [{#stats.msg_no_subscribers, 1}, {#stats.msg_dropped, 1}],
    update_counter(Tenant, UpdateOpt, #stats{
        tenant_id = Tenant, msg_no_subscribers = 1, msg_dropped = 1
    });
on_message_dropped(_, _, _) ->
    ok.

on_delivery_dropped(_, #message{flags = #{sys := true}}, _) ->
    ok;
on_delivery_dropped(_, #message{from = {Tenant, _}}, queue_full) ->
    UpdateOpt = [{#stats.msg_dropped_queue_full, 1}, {#stats.msg_dropped, 1}],
    update_counter(Tenant, UpdateOpt, #stats{
        tenant_id = Tenant, msg_dropped_queue_full = 1, msg_dropped = 1
    });
on_delivery_dropped(_, #message{from = {Tenant, _}}, expired) ->
    UpdateOpt = [{#stats.msg_dropped_expired, 1}, {#stats.msg_dropped, 1}],
    update_counter(Tenant, UpdateOpt, #stats{
        tenant_id = Tenant, msg_dropped_expired = 1, msg_dropped = 1
    });
on_delivery_dropped(_, #message{from = {Tenant, _}}, _) ->
    UpdateOpt = [{#stats.msg_dropped_expired, 1}, {#stats.msg_dropped, 1}],
    update_counter(Tenant, UpdateOpt, #stats{
        tenant_id = Tenant, msg_dropped = 1
    });
on_delivery_dropped(_, _, _) ->
    ok.

on_client_check_authz_complete(#{tenant_id := TenantId}, _PubSub, _Topic, Result, _AuthzSource) ->
    case Result of
        allow ->
            ok;
        deny ->
            update_counter(TenantId, [{#stats.acl_failed, 1}], #stats{
                tenant_id = TenantId, acl_failed = 1
            })
    end,
    ok.

on_client_check_authn_complete(#{clientid := {Tenant, _}}, {error, not_authorized}) ->
    update_counter(Tenant, [{#stats.auth_failed, 1}], #stats{
        tenant_id = Tenant, auth_failed = 1
    });
on_client_check_authn_complete(_, _) ->
    ok.

sample(Tenant) ->
    Sample1 =
        case ets:lookup(?SAMPLE, Tenant) of
            [] -> ?INIT;
            [{_, Sample0}] -> Sample0
        end,
    Stats =
        case ets:lookup(?STATS, Tenant) of
            [] -> #stats{tenant_id = Tenant};
            [Stats0] -> Stats0
        end,
    maps:merge(stats_to_map(Stats), Sample1).

update_rate_stats(Interval) ->
    ets:foldl(
        fun(Stats, Acc) ->
            #stats{
                tenant_id = Tenant,
                last_byte_sent = {ByteSent0, _},
                byte_sent = ByteSent,
                last_byte_received = {ByteRecv0, _},
                byte_received = ByteRecv,
                last_msg_sent = {MsgSent0, _},
                msg_sent = MsgSent,
                last_msg_received = {MsgRecv0, _},
                msg_received = MsgRecv,
                msg_dropped = MsgDropped,
                last_msg_dropped = {MsgDropped0, _}
            } = Stats,
            ets:update_element(?STATS, Tenant, [
                {#stats.last_byte_sent, {ByteSent, rate(ByteSent, ByteSent0, Interval)}},
                {#stats.last_byte_received, {ByteRecv, rate(ByteRecv, ByteRecv0, Interval)}},
                {#stats.last_msg_sent, {MsgSent, rate(MsgSent, MsgSent0, Interval)}},
                {#stats.last_msg_received, {MsgRecv, rate(MsgRecv, MsgRecv0, Interval)}},
                {#stats.last_msg_dropped, {MsgDropped, rate(MsgDropped, MsgDropped0, Interval)}}
            ]),
            Acc
        end,
        ok,
        ?STATS
    ).

update_stats({Tenant, _}, Stats) ->
    LastRecvOct = emqx_pd:get_counter(last_recv_oct),
    LastSendOct = emqx_pd:get_counter(last_send_oct),
    RecvOct = proplists:get_value(recv_oct, Stats, 0),
    SendOct = proplists:get_value(send_oct, Stats, 0),
    UpdateOpts = [
        {#stats.byte_received, RecvOct - LastRecvOct},
        {#stats.byte_sent, SendOct - LastSendOct}
    ],
    Default = #stats{
        tenant_id = Tenant,
        byte_received = RecvOct,
        byte_sent = SendOct
    },
    update_counter(Tenant, UpdateOpts, Default),
    emqx_pd:set_counter(last_recv_oct, RecvOct),
    emqx_pd:set_counter(last_send_oct, SendOct),
    ok;
update_stats(_, _) ->
    ok.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    ok = mria:wait_for_tables([?TENANCY]),
    erlang:process_flag(trap_exit, true),
    TabOpts = [public, {write_concurrency, true}],
    emqx_tables:new(?STATS, [{keypos, #stats.tenant_id} | TabOpts]),
    emqx_tables:new(?SAMPLE, [{keypos, 1} | TabOpts]),
    enable_hooks(),
    {ok, #{}, {continue, load_stats}}.

handle_continue(load_stats, State) ->
    Tenants = get_enabled_tenant(),
    lists:foreach(
        fun(Tenant) ->
            case mnesia:dirty_read(?DB, Tenant) of
                [] -> ok;
                [Stats] -> ets:insert(?STATS, Stats)
            end
        end,
        Tenants
    ),
    sample_timer(),
    save_timer(),
    {noreply, State, hibernate}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(sample, State) ->
    sample(),
    sample_timer(),
    {noreply, State};
handle_info(save_stats_to_db, State) ->
    save_stats_to_db(),
    save_timer(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    disable_hooks(),
    save_stats_to_db(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

enable_hooks() ->
    emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_TENANCY),
    emqx_hooks:put('message.dropped', {?MODULE, on_message_dropped, []}, ?HP_TENANCY),
    emqx_hooks:put('delivery.dropped', {?MODULE, on_delivery_dropped, []}, ?HP_TENANCY),
    emqx_hooks:put('message.delivered', {?MODULE, on_message_delivered, []}, ?HP_TENANCY),
    emqx_hooks:put(
        'client.check_authz_complete', {?MODULE, on_client_check_authz_complete, []}, ?HP_TENANCY
    ),
    emqx_hooks:put(
        'client.check_authn_complete', {?MODULE, on_client_check_authn_complete, []}, ?HP_TENANCY
    ).

disable_hooks() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    emqx_hooks:del('message.dropped', {?MODULE, on_message_dropped}),
    emqx_hooks:del('delivery.dropped', {?MODULE, on_delivery_dropped}),
    emqx_hooks:del('message.delivered', {?MODULE, on_message_delivered}),
    emqx_hooks:del('client.check_authz_complete', {?MODULE, on_client_check_authz_complete}),
    emqx_hooks:del('client.check_authn_complete', {?MODULE, on_client_check_authn_complete}),
    ok.

sample_timer() ->
    erlang:send_after(2000, self(), sample).

save_timer() ->
    erlang:send_after(60 * 1000, self(), save_stats_to_db).

sample() ->
    Session = sample_session(),
    Connection = sample_connection(),
    Topic = sample_topic(),
    {Subscription, SharedSubscription} = sample_subscription(),
    MsgRetained = sample_msg_retained(),
    Samples =
        [
            {sessions, Session},
            {connections, Connection},
            {topics, Topic},
            {subscriptions, Subscription},
            {subscriptions_shared, SharedSubscription},
            {msg_retained, MsgRetained}
        ],
    store_sample(merge_sample(Samples)),
    ok.

merge_sample(Samples) ->
    lists:foldl(
        fun({Key, Sample}, Acc) ->
            merge_sample(Sample, Key, Acc)
        end,
        #{},
        Samples
    ).

merge_sample(Sample, Key, Init) ->
    maps:fold(
        fun(S, Val, Acc) ->
            NewV =
                case maps:get(S, Acc, undefined) of
                    undefined -> ?INIT#{Key => Val};
                    V -> V#{Key => Val}
                end,
            maps:put(S, NewV, Acc)
        end,
        Init,
        Sample
    ).

store_sample(Sample) ->
    Tenants = get_enabled_tenant(),
    lists:foreach(
        fun(Tenant) ->
            New =
                case maps:get(Tenant, Sample, undefined) of
                    undefined -> {Tenant, ?INIT};
                    V -> {Tenant, V}
                end,
            ets:insert(?SAMPLE, New)
        end,
        Tenants
    ).

sample_session() ->
    ets:foldl(
        fun(Session, Acc) ->
            case Session of
                {{Tenant, _}, _} ->
                    maps:update_with(Tenant, fun(V) -> V + 1 end, 1, Acc);
                _ ->
                    Acc
            end
        end,
        #{},
        emqx_channel
    ).

sample_connection() ->
    ets:foldl(
        fun(Conn, Acc) ->
            case Conn of
                {{{Tenant, _}, _}, _} ->
                    maps:update_with(Tenant, fun(V) -> V + 1 end, 1, Acc);
                _ ->
                    Acc
            end
        end,
        #{},
        emqx_channel_conn
    ).

sample_topic() ->
    ets:foldl(
        fun(#route{topic = Topic}, Acc) ->
            case Topic of
                <<"$tenants/", Binary/binary>> ->
                    [Tenant, _] = binary:split(Binary, [<<"/">>]),
                    maps:update_with(Tenant, fun(V) -> V + 1 end, 1, Acc);
                _ ->
                    Acc
            end
        end,
        #{},
        emqx_route
    ).

sample_subscription() ->
    ets:foldl(
        fun(Sub, {Acc, ShareAcc}) ->
            case Sub of
                {_, #{subid := {Tenant, _}, share := _}} ->
                    {Acc, maps:update_with(Tenant, fun(V) -> V + 1 end, 1, ShareAcc)};
                {_, #{subid := {Tenant, _}}} ->
                    %% XXX: subscription should include shared subs?
                    {maps:update_with(Tenant, fun(V) -> V + 1 end, 1, Acc), ShareAcc};
                _ ->
                    {Acc, ShareAcc}
            end
        end,
        {#{}, #{}},
        emqx_suboption
    ).

sample_msg_retained() ->
    ets:foldl(
        fun(Msg, Acc) ->
            case Msg of
                {retained_message, _Topic, #message{from = {Tenant, _}}, _ExpireAt} ->
                    maps:update_with(Tenant, fun(V) -> V + 1 end, 1, Acc);
                _ ->
                    Acc
            end
        end,
        #{},
        emqx_retainer_message
    ).

update_counter(Tenant, UpdateOp, Default) ->
    _ = ets:update_counter(?STATS, Tenant, UpdateOp, Default),
    ok.

stats_to_map(Stats) ->
    #stats{
        tenant_id = TenantId,
        msg_received = MsgRecv,
        msg_sent = MsgSent,
        msg_dropped = MsgDropped,
        %%last_byte_sent = {_, ByteSentRate},
        %%last_byte_sent = {_, ByteSentRate},
        last_msg_received = {_, MsgRecvRate},
        last_msg_sent = {_, MsgSentRate},
        last_msg_dropped = {_, MsgDroppedRate},
        byte_received = ByteRecv,
        byte_sent = ByteSent,
        msg_dropped_queue_full = MsgDropQueueFull,
        msg_dropped_expired = MsgDropExpired,
        msg_no_subscribers = MsgNoSubscribers,
        acl_failed = AclFailed,
        auth_failed = AuthFailed,
        rule_action_failed = RuleActionFailed,
        rule_sql_failed = RuleSqlFailed,
        resource_failed = ResourceFailed
    } = Stats,
    #{
        tenant_id => TenantId,
        msg_received => MsgRecv,
        msg_sent => MsgSent,
        byte_received => ByteRecv,
        byte_sent => ByteSent,
        msg_dropped => MsgDropped,
        msg_dropped_queue_full => MsgDropQueueFull,
        msg_dropped_expired => MsgDropExpired,
        msg_no_subscribers => MsgNoSubscribers,
        acl_failed => AclFailed,
        auth_failed => AuthFailed,
        rule_action_failed => RuleActionFailed,
        rule_sql_failed => RuleSqlFailed,
        resource_failed => ResourceFailed,
        received_msg_rate => MsgRecvRate,
        sent_msg_rate => MsgSentRate,
        dropped_msg_rate => MsgDroppedRate
    }.

rate(Val1, Val0, Second) ->
    (Val1 - Val0) div Second.

get_enabled_tenant() ->
    Spec = ets:fun2ms(fun(#tenant{status = enabled, id = Id}) -> Id end),
    ets:select(?TENANCY, Spec).

save_stats_to_db() ->
    Tenants = get_enabled_tenant(),
    lists:foreach(
        fun(Tenant) ->
            case ets:lookup(?STATS, Tenant) of
                [] -> ok;
                [Stats] -> mnesia:dirty_write(?DB, Stats)
            end
        end,
        Tenants
    ).
