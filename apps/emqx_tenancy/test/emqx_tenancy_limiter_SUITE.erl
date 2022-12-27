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

-module(emqx_tenancy_limiter_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(TENANT_FOO, <<"tenant_foo">>).
-define(HOST, <<?TENANT_FOO/binary, <<".emqxserver.io">>/binary>>).

%%--------------------------------------------------------------------
%% setup
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_dashboard_api_test_helpers:init_suite(
        [emqx_conf, emqx_authn, emqx_authz, emqx_retainer, emqx_tenancy],
        fun set_special_configs/1
    ),
    Config.

end_per_suite(_) ->
    %% revert to default value
    {ok, _} = emqx:update_config(
        [authorization],
        #{
            <<"no_match">> => <<"allow">>,
            <<"cache">> => #{<<"enable">> => <<"true">>},
            <<"sources">> => []
        }
    ),
    emqx_dashboard_api_test_helpers:end_suite([
        emqx_tenancy, emqx_retainer, emqx_authz, emqx_authn, emqx_conf
    ]).

set_special_configs(emqx) ->
    %% restart gen_rpc with `stateless` mode
    application:set_env(gen_rpc, port_discovery, stateless),
    ok = application:stop(gen_rpc),
    ok = application:start(gen_rpc);
set_special_configs(emqx_authz) ->
    {ok, _} = emqx:update_config([authorization, sources], []);
set_special_configs(_) ->
    ok.

%%--------------------------------------------------------------------
%% tests
%%--------------------------------------------------------------------

t_limited(_) ->
    TenantIdA = <<"tenant_a">>,
    TenantIdB = <<"tenant_b">>,
    ok = create_tenant(TenantIdA, 5),
    ok = create_tenant(TenantIdB, 5),
    LmA = connect_limiter(TenantIdA),
    LmB = connect_limiter(TenantIdB),
    %% note: waiting 1.5s fullfill token bucket
    timer:sleep(1500),
    {ok, LmA1} = check_limiter(4, LmA),
    {ok, LmA2} = check_limiter(1, LmA1),
    %% request should be is suspended
    %% since the default capacity of tenant's bucket is equal to its rate.
    {pause, _PauseMs, _LmA3} = check_limiter(1, LmA2),
    %% Rate limiting segregation between tenants
    {ok, _LmB1} = check_limiter(4, LmB),
    ?assertMatch(#{}, emqx_tenancy_limiter:info(TenantIdA)),
    ?assertMatch(#{}, emqx_tenancy_limiter:info(TenantIdB)),

    ok = snabbkaffe:start_trace(),
    {ok, _} = ?block_until(#{?snk_kind := rebalanced}, 5500),
    snabbkaffe:stop(),

    ?assertMatch(#{}, emqx_tenancy_limiter:info(TenantIdA)),
    ?assertMatch(#{}, emqx_tenancy_limiter:info(TenantIdB)),

    ok.

t_misc(_) ->
    ?assertMatch(
        {reply, {error, unexpected_call}, state},
        emqx_tenancy_limiter_server:handle_call(unkown, self(), state)
    ),
    ?assertMatch(
        {noreply, state},
        emqx_tenancy_limiter_server:handle_cast(unkown, state)
    ),
    ?assertMatch(
        {noreply, state},
        emqx_tenancy_limiter_server:handle_info(unkown, state)
    ),
    ?assertMatch(
        {ok, state},
        emqx_tenancy_limiter_server:code_change(old, state, extra)
    ).

%%--------------------------------------------------------------------
%% helpers

%% create tenant
create_tenant(TenantId, MaxMsgIn) ->
    {ok, _} = emqx_tenancy:create(#{
        <<"id">> => TenantId,
        <<"configs">> => #{
            <<"limiters">> => #{
                <<"max_messages_in">> => MaxMsgIn,
                <<"max_bytes_in">> => 10 * 1024 * 1024
            }
        },
        <<"enabled">> => true,
        <<"desc">> => <<>>
    }),
    ok.

%%-------------------------------------------------------------------
%% limiter helpers

connect_limiter(TenantId) ->
    Limiter = emqx_limiter_container:get_limiter_by_types(
        {tcp, default},
        [message_in],
        #{
            client =>
                #{
                    message_in =>
                        #{
                            capacity => 1000,
                            divisible => true,
                            failure_strategy => force,
                            initial => 0,
                            low_watermark => 0,
                            max_retry_time => 10000,
                            rate => infinity
                        }
                },
            message_in => #{capacity => 1000, initial => 0, rate => infinity}
        }
    ),
    emqx_limiter_container:upgrade_with_tenant(TenantId, Limiter).

check_limiter(N, Limiter) ->
    emqx_limiter_container:check_list([{N, message_in}], Limiter).
