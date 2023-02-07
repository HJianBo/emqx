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

%% A hierarchical token bucket algorithm
%% Note: this is not the linux HTB algorithm(http://luxik.cdi.cz/~devik/qos/htb/manual/theory.htm)
%% Algorithm:
%% 1. the root node periodically generates tokens and then distributes them
%% just like the oscillation of water waves
%% 2. the leaf node has a counter, which is the place where the token is actually held.
%% 3. other nodes only play the role of transmission, and the rate of the node is like a valve,
%% limiting the oscillation transmitted from the parent node

-module(emqx_limiter_server).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,
    format_status/2
]).

-export([
    start_link/2,
    start_link/3,
    add_bucket/3,
    del_bucket/2,
    get_initial_val/1,
    whereis/1,
    info/1,
    name/1,
    restart/2,
    update_config/2,
    update_config/3,
    update_root_rate/3,
    update_buckets_rate/2
]).

%% number of tokens generated per period
-type root() :: #{
    rate := rate(),
    burst := rate(),
    %% token generation interval(second)
    period := pos_integer(),
    produced := float()
}.

-type bucket() :: #{
    name := bucket_name(),
    rate := rate(),
    obtained := float(),
    %% token correction value
    correction := emqx_limiter_decimal:zero_or_float(),
    capacity := capacity(),
    counter := undefined | counters:counters_ref(),
    index := undefined | index()
}.

-type state() :: #{
    type := limiter_type(),
    root := root(),
    buckets := buckets(),
    %% current counter to alloc
    counter := counters:counters_ref(),
    index := 0 | index()
}.

-type buckets() :: #{bucket_name() => bucket()}.
-type limiter_type() :: emqx_limiter_schema:limiter_type().
-type bucket_name() :: emqx_limiter_schema:bucket_name().
-type limiter_id() :: emqx_limiter_schema:limiter_id().
-type rate() :: decimal().
-type flow() :: decimal().
-type capacity() :: decimal().
-type decimal() :: emqx_limiter_decimal:decimal().
-type index() :: pos_integer().

%% minimum coefficient for overloaded limiter
-define(OVERLOAD_MIN_ALLOC, 0.3).
-define(COUNTER_SIZE, 8).

-export_type([index/0]).
-import(emqx_limiter_decimal, [add/2, sub/2, mul/2, put_to_counter/3]).

-elvis([{elvis_style, no_if_expression, disable}]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Add/Update bucket
-spec add_bucket(limiter_type() | pid(), limiter_id(), hocons:config() | undefined) -> ok.
add_bucket(_TypeOrPid, _Id, undefined) ->
    ok;
add_bucket(TypeOrPid, Id, Cfg) ->
    call(TypeOrPid, {add_bucket, Id, Cfg}).

-spec del_bucket(limiter_type() | pid(), limiter_id()) -> ok.
del_bucket(TypeOrPid, Id) ->
    call(TypeOrPid, {del_bucket, Id}).

-spec info(limiter_type() | pid()) -> state() | {error, _}.
info(TypeOrPid) ->
    call(TypeOrPid, info).

-spec restart(limiter_type() | pid(), hocons:config()) -> ok | {error, _}.
restart(TypeOrPid, Cfg) ->
    call(TypeOrPid, {restart, Cfg}).

-spec update_config(limiter_type(), hocons:config()) -> ok | {error, _}.
update_config(Type, Config) ->
    call(Type, {update_config, Type, Config}).

-spec update_config(pid(), limiter_type(), hocons:config()) -> ok | {error, _}.
update_config(Pid, Type, Config) ->
    call(Pid, {update_config, Type, Config}).

-spec update_root_rate(pid(), limiter_type(), rate()) -> ok | {error, _}.
update_root_rate(Pid, Type, Rate) ->
    call(Pid, {update_root_rate, Type, Rate}).

-spec update_buckets_rate(pid(), #{bucket_name() => rate()}) -> ok.
update_buckets_rate(Pid, Rates) ->
    call(Pid, {update_buckets_rate, Rates}).

-spec start_link(atom(), limiter_type(), hocons:config()) -> _.
start_link(noname, Type, Cfg) ->
    gen_server:start_link(?MODULE, [Type, Cfg], []).

-spec start_link(limiter_type(), hocons:config()) -> _.
start_link(Type, Cfg) ->
    gen_server:start_link({local, name(Type)}, ?MODULE, [Type, Cfg], []).

-spec whereis(limiter_type()) -> pid() | undefined.
whereis(Type) ->
    erlang:whereis(name(Type)).

-spec name(limiter_type()) -> atom().
name(Type) ->
    erlang:list_to_atom(io_lib:format("~s_~s", [?MODULE, Type])).

%%--------------------------------------------------------------------
%%% gen_server callbacks
%%--------------------------------------------------------------------

-spec init(Args :: term()) ->
    {ok, State :: term()}
    | {ok, State :: term(), Timeout :: timeout()}
    | {ok, State :: term(), hibernate}
    | {stop, Reason :: term()}
    | ignore.
init([Type, Cfg]) ->
    State = init_tree(Type, Cfg),
    #{root := #{period := Perido}} = State,
    oscillate(Perido),
    {ok, State}.

-spec handle_call(Request :: term(), From :: {pid(), term()}, State :: term()) ->
    {reply, Reply :: term(), NewState :: term()}
    | {reply, Reply :: term(), NewState :: term(), Timeout :: timeout()}
    | {reply, Reply :: term(), NewState :: term(), hibernate}
    | {noreply, NewState :: term()}
    | {noreply, NewState :: term(), Timeout :: timeout()}
    | {noreply, NewState :: term(), hibernate}
    | {stop, Reason :: term(), Reply :: term(), NewState :: term()}
    | {stop, Reason :: term(), NewState :: term()}.
handle_call(info, _From, State) ->
    {reply, State, State};
handle_call({restart, Cfg}, _From, #{type := Type}) ->
    %% XXX: why remove all buckets?
    NewState = init_tree(Type, Cfg),
    {reply, ok, NewState};
handle_call({update_config, Type, Config}, _From, #{type := Type}) ->
    %% XXX: why remove all buckets?
    NewState = init_tree(Type, Config),
    {reply, ok, NewState};
handle_call(
    {update_root_rate, Type, Rate},
    _From,
    State = #{type := Type, root := Root}
) ->
    NewState = State#{root := Root#{rate := Rate}},
    {reply, ok, NewState};
handle_call(
    {update_buckets_rate, Rates},
    _From,
    State
) ->
    NewState = do_update_buckets_rate(Rates, State),
    {reply, ok, NewState};
handle_call({add_bucket, Id, Cfg}, _From, State) ->
    NewState = do_add_bucket(Id, Cfg, State),
    {reply, ok, NewState};
handle_call({del_bucket, Id}, _From, State) ->
    NewState = do_del_bucket(Id, State),
    {reply, ok, NewState};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

-spec handle_cast(Request :: term(), State :: term()) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), Timeout :: timeout()}
    | {noreply, NewState :: term(), hibernate}
    | {stop, Reason :: term(), NewState :: term()}.
handle_cast(Req, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Req}),
    {noreply, State}.

-spec handle_info(Info :: timeout() | term(), State :: term()) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), Timeout :: timeout()}
    | {noreply, NewState :: term(), hibernate}
    | {stop, Reason :: normal | term(), NewState :: term()}.
handle_info(oscillate, State) ->
    {noreply, oscillation(State)};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

-spec terminate(
    Reason :: normal | shutdown | {shutdown, term()} | term(),
    State :: term()
) -> any().
terminate(_Reason, _State) ->
    ok.

-spec code_change(
    OldVsn :: term() | {down, term()},
    State :: term(),
    Extra :: term()
) ->
    {ok, NewState :: term()}
    | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec format_status(
    Opt :: normal | terminate,
    Status :: list()
) -> Status :: term().
format_status(_Opt, Status) ->
    Status.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
oscillate(Interval) ->
    erlang:send_after(Interval, self(), ?FUNCTION_NAME).

%% @doc generate tokens, and then spread to leaf nodes
-spec oscillation(state()) -> state().
oscillation(
    #{
        root := #{
            rate := Flow,
            period := Interval,
            produced := Produced
        } = Root,
        buckets := Buckets
    } = State
) ->
    oscillate(Interval),
    Ordereds = get_ordered_buckets(Buckets),
    {Alloced, Buckets2} = transverse(Ordereds, Flow, 0.0, Buckets),
    maybe_burst(State#{
        buckets := Buckets2,
        root := Root#{produced := Produced + Alloced}
    }).

%% @doc horizontal spread
-spec transverse(
    list(bucket()),
    flow(),
    float(),
    buckets()
) -> {float(), buckets()}.
transverse([H | T], InFlow, Alloced, Buckets) when InFlow > 0 ->
    {BucketAlloced, Buckets2} = longitudinal(H, InFlow, Buckets),
    InFlow2 = sub(InFlow, BucketAlloced),
    Alloced2 = Alloced + BucketAlloced,
    transverse(T, InFlow2, Alloced2, Buckets2);
transverse(_, _, Alloced, Buckets) ->
    {Alloced, Buckets}.

%% @doc vertical spread
-spec longitudinal(bucket(), flow(), buckets()) ->
    {float(), buckets()}.
longitudinal(
    #{
        name := Name,
        rate := Rate,
        capacity := Capacity,
        counter := Counter,
        index := Index,
        obtained := Obtained
    } = Bucket,
    InFlow,
    Buckets
) when Counter =/= undefined ->
    Flow = erlang:min(InFlow, Rate),

    ShouldAlloc =
        case counters:get(Counter, Index) of
            Tokens when Tokens < 0 ->
                %% toknes's value mayb be a negative value(stolen from the future)
                %% because ∃ x. add(Capacity, x) < 0, so here we must compare with minimum value
                erlang:max(
                    add(Capacity, Tokens),
                    mul(Capacity, ?OVERLOAD_MIN_ALLOC)
                );
            Tokens ->
                %% is it possible that Tokens > Capacity ???
                erlang:max(sub(Capacity, Tokens), 0)
        end,

    case lists:min([ShouldAlloc, Flow, Capacity]) of
        Available when Available > 0 ->
            {Inc, Bucket2} = emqx_limiter_correction:add(Available, Bucket),
            counters:add(Counter, Index, Inc),

            {Available, Buckets#{Name := Bucket2#{obtained := Obtained + Available}}};
        _ ->
            {0, Buckets}
    end;
longitudinal(_, _, Buckets) ->
    {0, Buckets}.

-spec get_ordered_buckets(list(bucket()) | buckets()) -> list(bucket()).
get_ordered_buckets(Buckets) when is_map(Buckets) ->
    BucketList = maps:values(Buckets),
    get_ordered_buckets(BucketList);
get_ordered_buckets(Buckets) ->
    %% sort by obtained, avoid node goes hungry
    lists:sort(
        fun(#{obtained := A}, #{obtained := B}) ->
            A < B
        end,
        Buckets
    ).

-spec maybe_burst(state()) -> state().
maybe_burst(
    #{
        buckets := Buckets,
        root := #{burst := Burst}
    } = State
) when Burst > 0 ->
    Fold = fun
        (_Name, #{counter := Cnt, index := Idx} = Bucket, Acc) when Cnt =/= undefined ->
            case counters:get(Cnt, Idx) > 0 of
                true ->
                    Acc;
                false ->
                    [Bucket | Acc]
            end;
        (_Name, _Bucket, Acc) ->
            Acc
    end,

    Empties = maps:fold(Fold, [], Buckets),
    dispatch_burst(Empties, Burst, State);
maybe_burst(State) ->
    State.

-spec dispatch_burst(list(bucket()), non_neg_integer(), state()) -> state().
dispatch_burst([], _, State) ->
    State;
dispatch_burst(
    Empties,
    InFlow,
    #{root := #{produced := Produced} = Root, buckets := Buckets} = State
) ->
    EachFlow = InFlow / erlang:length(Empties),
    {Alloced, Buckets2} = dispatch_burst_to_buckets(Empties, EachFlow, 0, Buckets),
    State#{root := Root#{produced := Produced + Alloced}, buckets := Buckets2}.

-spec dispatch_burst_to_buckets(
    list(bucket()),
    float(),
    non_neg_integer(),
    buckets()
) -> {non_neg_integer(), buckets()}.
dispatch_burst_to_buckets([Bucket | T], InFlow, Alloced, Buckets) ->
    #{
        name := Name,
        counter := Counter,
        index := Index,
        obtained := Obtained
    } = Bucket,
    {Inc, Bucket2} = emqx_limiter_correction:add(InFlow, Bucket),

    counters:add(Counter, Index, Inc),

    Buckets2 = Buckets#{Name := Bucket2#{obtained := Obtained + Inc}},
    dispatch_burst_to_buckets(T, InFlow, Alloced + Inc, Buckets2);
dispatch_burst_to_buckets([], _, Alloced, Buckets) ->
    {Alloced, Buckets}.

-spec init_tree(emqx_limiter_schema:limiter_type(), hocons:config()) -> state().
init_tree(Type, Cfg) when is_atom(Type) ->
    #{
        type => Type,
        root => make_root(Cfg),
        counter => counters:new(?COUNTER_SIZE, [write_concurrency]),
        index => 0,
        buckets => #{}
    }.

-spec make_root(hocons:confg()) -> root().
make_root(#{rate := Rate, burst := Burst} = Conf) ->
    #{
        rate => Rate,
        burst => Burst,
        period => maps:get(period, Conf, emqx_limiter_schema:default_period()),
        produced => 0.0
    }.

do_add_bucket(Id, #{rate := Rate, capacity := Capacity} = Cfg, #{buckets := Buckets} = State) ->
    case maps:get(Id, Buckets, undefined) of
        undefined ->
            make_bucket(Id, Cfg, State);
        Bucket ->
            Bucket2 = Bucket#{rate := Rate, capacity := Capacity},
            State#{buckets := Buckets#{Id := Bucket2}}
    end.

make_bucket(Id, Cfg, #{index := ?COUNTER_SIZE} = State) ->
    make_bucket(Id, Cfg, State#{
        counter => counters:new(?COUNTER_SIZE, [write_concurrency]),
        index => 0
    });
make_bucket(
    Id,
    #{rate := Rate, capacity := Capacity} = Cfg,
    #{type := Type, counter := Counter, index := Index, buckets := Buckets} = State
) ->
    NewIndex = Index + 1,
    Initial = get_initial_val(Cfg),
    Bucket = #{
        name => Id,
        rate => Rate,
        obtained => Initial,
        correction => 0,
        capacity => Capacity,
        counter => Counter,
        index => NewIndex
    },
    _ = put_to_counter(Counter, NewIndex, Initial),
    Ref = emqx_limiter_bucket_ref:new(Counter, NewIndex, Rate),
    emqx_limiter_manager:insert_bucket(Id, Type, Ref),
    State#{buckets := Buckets#{Id => Bucket}, index := NewIndex}.

do_del_bucket(Id, #{type := Type, buckets := Buckets} = State) ->
    case maps:get(Id, Buckets, undefined) of
        undefined ->
            State;
        _ ->
            emqx_limiter_manager:delete_bucket(Id, Type),
            State#{buckets := maps:remove(Id, Buckets)}
    end.

do_update_buckets_rate(Rates, #{buckets := Buckets} = State) ->
    Buckets1 =
        maps:fold(
            fun(Id, Rate, Acc) ->
                case maps:get(Id, Acc, undefined) of
                    undefined ->
                        Acc;
                    Bucket ->
                        Acc#{Id := Bucket#{rate := Rate}}
                end
            end,
            Buckets,
            Rates
        ),
    State#{buckets := Buckets1}.

-spec get_initial_val(hocons:config()) -> decimal().
get_initial_val(
    #{
        initial := Initial,
        rate := Rate,
        capacity := Capacity
    }
) ->
    %% initial will nevner be infinity(see the emqx_limiter_schema)
    InfVal = emqx_limiter_schema:infinity_value(),
    if
        Initial > 0 ->
            Initial;
        Rate =/= infinity ->
            erlang:min(Rate, Capacity);
        Capacity =/= infinity andalso Capacity =/= InfVal ->
            Capacity;
        true ->
            0
    end.

-spec call(limiter_type() | pid(), any()) -> {error, _} | _.
call(Type, Msg) when is_atom(Type) ->
    case ?MODULE:whereis(Type) of
        undefined ->
            {error, limiter_not_started};
        Pid ->
            gen_server:call(Pid, Msg)
    end;
call(Pid, Msg) ->
    gen_server:call(Pid, Msg).
