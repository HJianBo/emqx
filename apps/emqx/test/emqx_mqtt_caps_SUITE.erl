%%--------------------------------------------------------------------
%% Copyright (c) 2018-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mqtt_caps_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

t_check_pub(_) ->
    OldConf = emqx_config:get([zones]),
    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], ?QOS_1),
    emqx_config:put_zone_conf(default, [mqtt, retain_available], false),
    timer:sleep(50),
    ok = emqx_mqtt_caps:check_pub(default, #{qos => ?QOS_1, retain => false}),
    PubFlags1 = #{qos => ?QOS_2, retain => false},
    ?assertEqual({error, ?RC_QOS_NOT_SUPPORTED},
                 emqx_mqtt_caps:check_pub(default, PubFlags1)),
    PubFlags2 = #{qos => ?QOS_1, retain => true},
    ?assertEqual({error, ?RC_RETAIN_NOT_SUPPORTED},
                 emqx_mqtt_caps:check_pub(default, PubFlags2)),
    emqx_config:put([zones], OldConf).

t_check_sub(_) ->
    OldConf = emqx_config:get([zones]),
    SubOpts = #{rh  => 0,
                rap => 0,
                nl  => 0,
                qos => ?QOS_2
               },
    emqx_config:put_zone_conf(default, [mqtt, max_topic_levels], 2),
    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], ?QOS_1),
    emqx_config:put_zone_conf(default, [mqtt, shared_subscription], false),
    emqx_config:put_zone_conf(default, [mqtt, wildcard_subscription], false),
    timer:sleep(50),
    ok = emqx_mqtt_caps:check_sub(default, <<"topic">>, SubOpts),
    ?assertEqual({error, ?RC_TOPIC_FILTER_INVALID},
                 emqx_mqtt_caps:check_sub(default, <<"a/b/c/d">>, SubOpts)),
    ?assertEqual({error, ?RC_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED},
                 emqx_mqtt_caps:check_sub(default, <<"+/#">>, SubOpts)),
    ?assertEqual({error, ?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED},
                 emqx_mqtt_caps:check_sub(default, <<"topic">>, SubOpts#{share => true})),
    emqx_config:put([zones], OldConf).
