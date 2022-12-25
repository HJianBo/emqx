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

-module(emqx_tenancy_quota_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TENANT_FOO, <<"tenant_foo">>).

%%--------------------------------------------------------------------
%% setup
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_dashboard_api_test_helpers:init_suite(
        [emqx_conf, emqx_authn, emqx_authz, emqx_tenancy],
        fun set_special_configs/1
    ),
    %% create tenant
    {ok, _} = emqx_tenancy:create(#{
        <<"id">> => ?TENANT_FOO,
        <<"configs">> => #{
            <<"quotas">> => #{
                <<"max_sessions">> => 1,
                <<"max_auhtn_users">> => 1,
                <<"max_authz_rules">> => 1
            }
        },
        <<"status">> => enabled,
        <<"desc">> => <<>>
    }),
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
    ok = emqx_tenancy:delete(?TENANT_FOO),
    emqx_dashboard_api_test_helpers:end_suite([emqx_tenancy, emqx_authz, emqx_authn, emqx_conf]).

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

t_no_submit_delay_on_single_node(_) ->
    allow = emqx_hooks:run_fold('quota.sessions', [acquire, clientinfo()], deny),
    deny = emqx_hooks:run_fold('quota.sessions', [acquire, clientinfo()], deny),
    deny = emqx_hooks:run_fold('quota.sessions', [acquire, clientinfo()], allow),
    ok.

t_connection_crash(_) ->
    ok.

t_reload_authn_authz_usage(_) ->
    ok.

%%--------------------------------------------------------------------
%% helpers

clientinfo() ->
    #{
        tenant_id => ?TENANT_FOO,
        clientid => emqx_clientid:with_tenant(?TENANT_FOO, <<"clientid">>)
    }.
