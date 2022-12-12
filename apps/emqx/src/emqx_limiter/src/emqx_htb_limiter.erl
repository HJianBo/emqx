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

-module(emqx_htb_limiter).

-include_lib("emqx/include/logger.hrl").

%% @doc the limiter of the hierarchical token limiter system
%% this module provides api for creating limiters, consume tokens, check tokens and retry
%% @end

%% API
-export([
    connect/3,
    check/2,
    consume/2,
    set_retry/2,
    retry/1,
    retry/2,
    make_future/1,
    available/1
]).

-export([
    connect_to_extra/3,
    set_extra_buckets/2
]).

%% low-level exports
-export([
    make_token_bucket_limiter/2,
    make_ref_limiter/2,
    make_infinity_limiter/0
]).

-export_type([token_bucket_limiter/0]).

%% a token bucket limiter with a limiter server's bucket reference

%% the number of tokens currently available
-type token_bucket_limiter() :: #{
    tokens := non_neg_integer(),
    rate := decimal(),
    capacity := decimal(),
    lasttime := millisecond(),
    %% @see emqx_limiter_schema
    max_retry_time := non_neg_integer(),
    %% @see emqx_limiter_schema
    failure_strategy := failure_strategy(),
    %% @see emqx_limiter_schema
    divisible := boolean(),
    %% @see emqx_limiter_schema
    low_watermark := non_neg_integer(),
    %% the limiter server's bucket
    bucket := bucket(),
    %% extra buckets controlled by others apps
    extra_buckets := list(bucket()),

    %% retry contenxt
    %% undefined meaning no retry context or no need to retry
    retry_ctx =>
        undefined
        %% the retry context
        | retry_context(),
    %% allow to add other keys
    atom() => any()
}.

%% a limiter server's bucket reference
-type ref_limiter() :: #{
    max_retry_time := non_neg_integer(),
    failure_strategy := failure_strategy(),
    divisible := boolean(),
    low_watermark := non_neg_integer(),
    bucket := bucket(),
    extra_buckets := list(bucket()),

    retry_ctx => undefined | retry_context(),
    %% allow to add other keys
    atom => any()
}.

-type bucket() :: emqx_limiter_bucket_ref:bucket_ref().
-type limiter() :: token_bucket_limiter() | ref_limiter() | infinity.
-type millisecond() :: non_neg_integer().

-type consume_array() :: list({BucketIndex :: pos_integer(), Need :: pos_integer()}).

-type retry_context() ::
    #{
        consume_array => consume_array(),
        min_left => pos_integer(),
        waiting_local_bucket => pos_integer(),
        need => pos_integer(),
        start => millisecond()
    }.

-type pause_type() :: pause | partial.
-type check_result_ok(Limiter) :: {ok, Limiter}.
-type check_result_pause(Limiter) :: {pause_type(), millisecond(), retry_context(), Limiter}.
-type result_drop(Limiter) :: {drop, Limiter}.

-type check_result(Limiter) ::
    check_result_ok(Limiter)
    | check_result_pause(Limiter)
    | result_drop(Limiter).

-type inner_check_result(Limiter) ::
    check_result_ok(Limiter)
    | check_result_pause(Limiter).

-type consume_result(Limiter) ::
    check_result_ok(Limiter)
    | result_drop(Limiter).

-type decimal() :: emqx_limiter_decimal:decimal().
-type failure_strategy() :: emqx_limiter_schema:failure_strategy().

-type limiter_bucket_cfg() :: #{
    rate := decimal(),
    initial := non_neg_integer(),
    low_watermark := non_neg_integer(),
    capacity := decimal(),
    divisible := boolean(),
    max_retry_time := non_neg_integer(),
    failure_strategy := failure_strategy()
}.

-type future() :: pos_integer().

-define(NOW, erlang:monotonic_time(millisecond)).
-define(MINIMUM_PAUSE, 50).
-define(MAXIMUM_PAUSE, 5000).

-import(emqx_limiter_decimal, [sub/2, mul/2, floor_div/2, add/2]).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

-type limiter_id() :: emqx_limiter_schema:limiter_id().
-type limiter_type() :: emqx_limiter_schema:bucket_name().
-type bucket_name() :: emqx_limiter_schema:bucket_name().

%% @doc Connect to a limiter bucket and create a local token bucket
-spec connect(
    limiter_id(),
    limiter_type(),
    bucket_name() | #{limiter_type() => bucket_name() | undefined}
) ->
    {ok, limiter()} | {error, _}.
connect(_Id, _Type, undefined) ->
    %% If no bucket path is set in config, there will be no limit
    {ok, emqx_htb_limiter:make_infinity_limiter()};
connect(Id, Type, Cfg) ->
    case find_limiter_cfg(Type, Cfg) of
        {undefined, _} ->
            {ok, emqx_htb_limiter:make_infinity_limiter()};
        {
            #{
                rate := BucketRate,
                capacity := BucketSize
            },
            #{rate := CliRate, capacity := CliSize} = ClientCfg
        } ->
            case emqx_limiter_manager:find_bucket(Id, Type) of
                {ok, Bucket} ->
                    {ok,
                        if
                            CliRate < BucketRate orelse CliSize < BucketSize ->
                                emqx_htb_limiter:make_token_bucket_limiter(ClientCfg, Bucket);
                            true ->
                                emqx_htb_limiter:make_ref_limiter(ClientCfg, Bucket)
                        end};
                undefined ->
                    ?SLOG(error, #{msg => "bucket_not_found", type => Type, id => Id}),
                    {error, invalid_bucket}
            end
    end.

%% @doc create a limiter
-spec make_token_bucket_limiter(limiter_bucket_cfg(), bucket()) -> _.
make_token_bucket_limiter(Cfg, Bucket) ->
    Cfg#{
        tokens => emqx_limiter_server:get_initial_val(Cfg),
        lasttime => ?NOW,
        bucket => Bucket,
        extra_buckets => []
    }.

%% @doc create a limiter server's reference
-spec make_ref_limiter(limiter_bucket_cfg(), bucket()) -> ref_limiter().
make_ref_limiter(Cfg, Bucket) when Bucket =/= infinity ->
    Cfg#{bucket => Bucket, extra_buckets => []}.

-spec make_infinity_limiter() -> infinity.
make_infinity_limiter() ->
    infinity.

%% Cfg :: limiter_bucket_cfg()
find_limiter_cfg(Type, #{rate := _} = Cfg) ->
    {Cfg, find_client_cfg(Type, maps:get(client, Cfg, undefined))};
%% Cfg :: #{limiter_type() => limiter_bucket_cfg()}, provided by emqx_limiter_container
find_limiter_cfg(Type, Cfg) ->
    {
        maps:get(Type, Cfg, undefined),
        find_client_cfg(Type, emqx_map_lib:deep_get([client, Type], Cfg, undefined))
    }.

find_client_cfg(Type, BucketCfg) ->
    NodeCfg = emqx:get_config([limiter, client, Type], undefined),
    merge_client_cfg(NodeCfg, BucketCfg).

merge_client_cfg(undefined, BucketCfg) ->
    BucketCfg;
merge_client_cfg(NodeCfg, undefined) ->
    NodeCfg;
merge_client_cfg(NodeCfg, BucketCfg) ->
    maps:merge(NodeCfg, BucketCfg).

%% @doc request some tokens
%% it will automatically retry when failed until the maximum retry time is reached
%% @end
-spec consume(integer(), Limiter) -> consume_result(Limiter) when
    Limiter :: limiter().
consume(Need, #{max_retry_time := RetryTime} = Limiter) when Need > 0 ->
    try_consume(RetryTime, Need, Limiter);
consume(_, Limiter) ->
    {ok, Limiter}.

%% @doc try to request the token and return the result without automatically retrying
-spec check(pos_integer() | retry_context(), Limiter) -> check_result(Limiter) when
    Limiter :: limiter().
check(_, infinity) ->
    {ok, infinity};
check(Need, Limiter) when is_integer(Need), Need > 0 ->
    case do_check(Need, Limiter) of
        {ok, _} = Done ->
            Done;
        {PauseType, Pause, RetryCtx, Limiter2} ->
            {PauseType, Pause, RetryCtx#{start => ?NOW, need => Need}, Limiter2}
    end;
check(Need, Limiter) when is_map(Need) ->
    retry(Need, Limiter);
check(Need, Limiter) when is_integer(Need) ->
    {ok, Limiter}.

%% @doc pack the retry context into the limiter data
-spec set_retry(retry_context(), Limiter) -> Limiter when
    Limiter :: limiter().
set_retry(Retry, Limiter) ->
    Limiter#{retry_ctx => Retry}.

%% @doc check if there is a retry context, and try again if there is
-spec retry(Limiter) -> check_result(Limiter) when Limiter :: limiter().
retry(#{retry_ctx := Retry} = Limiter) when is_map(Retry) ->
    retry(Retry, Limiter#{retry_ctx := undefined});
retry(Limiter) ->
    {ok, Limiter}.

%% @doc retry with retry context
-spec retry(retry_context(), Limiter) -> check_result(Limiter) when Limiter :: limiter().
retry(RetryCtx = #{waiting_local_bucket := Diff}, Limiter) ->
    Result = do_reset(Diff, Limiter),
    may_drop_failure(RetryCtx, Result);
retry(#{consume_array := []}, Limiter) ->
    {ok, Limiter};
retry(RetryCtx = #{consume_array := RetryArray, min_left := RefMinLeft0}, Limiter) ->
    Result = do_check_all_ref_buckets(RetryArray, RefMinLeft0, Limiter),
    may_drop_failure(RetryCtx, Result).

%% @doc make a future value
%% this similar to retry context, but represents a value that will be checked in the future
%% @end
-spec make_future(pos_integer()) -> future().
make_future(Need) ->
    Need.

%% @doc get the number of tokens currently available
-spec available(limiter()) -> decimal().
available(#{
    tokens := Tokens,
    rate := Rate,
    lasttime := LastTime,
    capacity := Capacity,
    bucket := Bucket
}) ->
    Tokens2 = apply_elapsed_time(Rate, ?NOW - LastTime, Tokens, Capacity),
    erlang:min(Tokens2, emqx_limiter_bucket_ref:available(Bucket));
available(#{bucket := Bucket}) ->
    emqx_limiter_bucket_ref:available(Bucket);
available(infinity) ->
    infinity.

%% @doc Connect to created reference buckets
-spec connect_to_extra(limiter_id(), limiter_type(), limiter()) -> limiter().
connect_to_extra(Id, Type, Limiter) ->
    %% assert: retry_ctx must be undefined. Since modifying buckets list
    %% during retry is not supported yet
    undefined = maps:get(retry_ctx, Limiter, undefined),
    case emqx_limiter_manager:find_bucket(Id, Type) of
        {ok, Bucket} ->
            Limiter#{extra_buckets := [Bucket]};
        undefined ->
            Limiter
    end.

-spec set_extra_buckets(list(bucket()), limiter()) -> limiter().
set_extra_buckets(Buckets, Limiter) ->
    Limiter#{extra_buckets => Buckets}.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
-spec try_consume(
    millisecond(),
    retry_context() | pos_integer(),
    Limiter
) -> consume_result(Limiter) when Limiter :: limiter().
try_consume(LeftTime, Retry, #{failure_strategy := Failure} = Limiter) when
    LeftTime =< 0, is_map(Retry)
->
    on_failure(Failure, try_restore(Retry, Limiter));
try_consume(LeftTime, Need, Limiter) when is_integer(Need) ->
    case check(Need, Limiter) of
        {ok, _} = Done ->
            Done;
        {_, Pause, RetryCtx, Limiter2} ->
            timer:sleep(erlang:min(LeftTime, Pause)),
            try_consume(LeftTime - Pause, RetryCtx, Limiter2)
    end;
try_consume(
    LeftTime,
    RetryCtx,
    Limiter
) ->
    case retry(RetryCtx, Limiter) of
        {ok, _} = Done ->
            Done;
        {_, Pause, NRetryCtx, Limiter2} ->
            timer:sleep(erlang:min(LeftTime, Pause)),
            try_consume(LeftTime - Pause, NRetryCtx, Limiter2)
    end.

-spec do_check(pos_integer(), Limiter) -> inner_check_result(Limiter) when
    Limiter :: limiter().
do_check(Need, #{tokens := Tokens} = Limiter) when Need =< Tokens ->
    do_check_with_parent_limiter(Need, Limiter);
do_check(Need, #{tokens := _} = Limiter) ->
    do_reset(Need, Limiter);
do_check(Need, Limiter) ->
    CheckAll = check_all_consume_array(Need, Limiter),
    do_check_all_ref_buckets(CheckAll, infinity, Limiter).

on_failure(force, Limiter) ->
    {ok, Limiter};
on_failure(drop, Limiter) ->
    {drop, Limiter};
on_failure(throw, Limiter) ->
    Message = io_lib:format("limiter consume failed, limiter:~p~n", [Limiter]),
    erlang:throw({rate_check_fail, Message}).

do_check_with_parent_limiter(
    Need,
    #{
        tokens := Tokens
    } = Limiter
) ->
    Left = sub(Tokens, Need),
    CheckAll = check_all_consume_array(Need, Limiter),
    do_check_all_ref_buckets(CheckAll, infinity, Limiter#{tokens := Left}).

may_drop_failure(_, Done = {ok, _}) ->
    Done;
may_drop_failure(
    LastRetryCtx = #{start := Start},
    {PauseType, PauseMs, RetryCtx,
        #{
            failure_strategy := Failure,
            max_retry_time := RetryTime
        } = Limiter}
) ->
    NRetryCtx = maps:merge(LastRetryCtx, RetryCtx),
    case ?NOW - Start >= RetryTime of
        false ->
            {PauseType, PauseMs, NRetryCtx, Limiter};
        _ ->
            on_failure(Failure, try_restore(NRetryCtx, Limiter))
    end.

do_check_all_ref_buckets([], _, Limiter) ->
    {ok, Limiter};
do_check_all_ref_buckets(
    ConsumeArray,
    MinLeft,
    #{
        bucket := Bucket0,
        extra_buckets := Buckets0,
        divisible := Divisible
    } = Limiter
) ->
    Buckets = [Bucket0 | Buckets0],
    Results = lists:map(
        fun({Index, Need}) ->
            PerBucket = lists:nth(Index, Buckets),
            {Index, Need, emqx_limiter_bucket_ref:check(Need, PerBucket, Divisible)}
        end,
        ConsumeArray
    ),
    {RefMinLeft, MaxPauseType, MaxPauseMs} = analyze_check_results(
        Results,
        {MinLeft, partial, ?MINIMUM_PAUSE}
    ),
    case
        lists:all(
            fun
                ({_, _, {ok, _}}) -> true;
                (_) -> false
            end,
            Results
        )
    of
        true ->
            Left = erlang:min(RefMinLeft, maps:get(tokens, Limiter, infinity)),
            may_low_watermark_pause(Left, Limiter);
        false ->
            RetryCtx = make_retry_context(
                generate_retry_array(Results),
                RefMinLeft
            ),
            {MaxPauseType, MaxPauseMs, RetryCtx, Limiter}
    end.

generate_retry_array(Results) ->
    lists:reverse(
        lists:foldl(
            fun
                ({_Index, _Need, {ok, _}}, Ls) ->
                    Ls;
                ({Index, Need, {_PauseType, _Rate, Obtained}}, Ls) ->
                    [{Index, Need - Obtained} | Ls]
            end,
            [],
            Results
        )
    ).

analyze_check_results([], Acc) ->
    Acc;
analyze_check_results(
    [{_Index, _Need, {ok, Left}} | Results],
    Acc = {MinLeft, MaxPauseType, MaxPauseMs}
) ->
    NAcc =
        case Left < MinLeft of
            true ->
                {Left, MaxPauseType, MaxPauseMs};
            false ->
                Acc
        end,
    analyze_check_results(Results, NAcc);
analyze_check_results(
    [{_Index, _Need, {_PauseType, infinity, _Obtained}} | Results],
    Acc
) ->
    analyze_check_results(Results, Acc);
analyze_check_results(
    [{_Index, Need, {PauseType, Rate, Obtained}} | Results],
    {MinLeft, MaxPauseType, MaxPauseMs}
) ->
    NMaxPauseType = max_pause_type(PauseType, MaxPauseType),
    Pause = calc_pause_ms(Need - Obtained, Rate),
    NMaxPauseMs =
        case Pause > MaxPauseMs of
            true -> Pause;
            false -> MaxPauseMs
        end,
    analyze_check_results(Results, {MinLeft, NMaxPauseType, NMaxPauseMs}).

max_pause_type(T, T) -> T;
max_pause_type(_, pause) -> pause;
max_pause_type(pause, _T) -> pause.

-spec do_reset(pos_integer(), token_bucket_limiter()) -> inner_check_result(token_bucket_limiter()).
do_reset(
    Need,
    #{
        tokens := Tokens,
        rate := Rate,
        lasttime := LastTime,
        divisible := Divisible,
        capacity := Capacity
    } = Limiter
) ->
    Now = ?NOW,
    Tokens2 = apply_elapsed_time(Rate, Now - LastTime, Tokens, Capacity),

    case erlang:floor(Tokens2) of
        Available when Available >= Need ->
            Limiter2 = Limiter#{tokens := Tokens2, lasttime := Now},
            do_check_with_parent_limiter(Need, Limiter2);
        Available when Divisible andalso Available > 0 ->
            PauseMs = calc_pause_ms(Need - Available, Rate),
            RetryCtx = #{waiting_local_bucket => Need - Available},
            {partial, PauseMs, RetryCtx, Limiter#{tokens := 0, lasttime := Now}};
        _ ->
            PauseMs = calc_pause_ms(Need, Rate),
            RetryCtx = #{waiting_local_bucket => Need},
            {pause, PauseMs, RetryCtx, Limiter}
    end.

calc_pause_ms(Need, Rate) ->
    Val = erlang:round((Need) * emqx_limiter_schema:default_period() / Rate),
    emqx_misc:clamp(Val, ?MINIMUM_PAUSE, ?MAXIMUM_PAUSE).

check_all_consume_array(Need, #{bucket := Bucket, extra_buckets := ExtraBuckets}) ->
    Count =
        length(ExtraBuckets) +
            case Bucket of
                undefined -> 0;
                _ -> 1
            end,
    [{I, Need} || I <- lists:seq(1, Count)].

-spec try_restore(retry_context(), Limiter) -> Limiter when
    Limiter :: limiter().
try_restore(
    #{need := Need, waiting_local_bucket := Diff},
    #{tokens := Tokens, capacity := Capacity, bucket := Bucket} = Limiter
) ->
    Back = Need - Diff,
    Tokens2 = erlang:min(Capacity, Back + Tokens),
    emqx_limiter_bucket_ref:try_restore(Back, Bucket),
    Limiter#{tokens := Tokens2};
try_restore(#{need := Need, consume_array := _ConsumeArray}, #{bucket := Bucket} = Limiter) ->
    %% TODO: need to calculate return num for each buckets
    _ = emqx_limiter_bucket_ref:try_restore(Need, Bucket),
    Limiter.

may_low_watermark_pause(Left, #{low_watermark := Mark} = Limiter) when Left >= Mark ->
    {ok, Limiter};
may_low_watermark_pause(Left, Limiter) ->
    {pause, ?MINIMUM_PAUSE, make_retry_context([], Left), Limiter}.

make_retry_context(ConsumeArray, MinLeft) ->
    #{
        consume_array => ConsumeArray,
        min_left => MinLeft
    }.

%% @doc apply the elapsed time to the limiter
apply_elapsed_time(Rate, Elapsed, Tokens, Capacity) ->
    Inc = floor_div(mul(Elapsed, Rate), emqx_limiter_schema:default_period()),
    erlang:min(add(Tokens, Inc), Capacity).
