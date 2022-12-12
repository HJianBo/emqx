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

-ifndef(EMQX_TENANCY_HRL).
-define(EMQX_TENANCY_HRL, true).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-define(TENANCY_SHARD, emqx_tenancy_shard).

-type tenant_id() :: binary().

-type config() :: quota_config() | limiter_config() | common_config().

-type quota_config() ::
    #{
        max_connections := pos_integer(),
        max_authn_users := pos_integer(),
        max_authz_users := pos_integer()
        %max_subscriptions := pos_integer(),
        %max_retained_messages := pos_integer(),
        %max_rules := pos_integer(),
        %max_resources := pos_integer(),
        %max_shared_subscriptions := pos_integer()
    }.

-type limiter_config() ::
    #{
        max_messages_in := pos_integer(),
        max_bytes_in := pos_integer()
        %max_conn_rate := pos_integer(),
        %%max_sub_rate := pos_integer() not support now
    }.

-type common_config() ::
    #{
        %min_keepalive := pos_integer(),
        %max_keepalive := pos_integer(),
        %session_expiry_interval := pos_integer(),
        %max_mqueue_len := pos_integer(),
        %max_inflight := pos_integer(),
        %max_awaiting_rel := pos_integer(),
        %max_packet_size := pos_integer(),
        %max_clientid_len := pos_integer(),
        %max_topic_levels := pos_integer(),
        %max_qos_allowed := pos_integer(),
        %max_topic_alias := pos_integer()
        atom() => term()
    }.

%% @doc node level limiter info
-type limiter_info() :: #{
    type := limiter_type(),
    %% All rate limite around cluster
    rate := pos_integer(),
    %% Allocated rate for current node
    allocated_rate := pos_integer(),
    %% Rate occurred on the cluster in last 5s
    latest_cluster_rate := float(),
    %% Rate occurred on this node in last 5s
    latest_node_rate := float(),
    %% Number of assigned tokens
    obtained := float()
}.

-record(tenant, {
    id :: tenant_id() | '_' | undefined,
    configs :: config() | '_' | undefined,
    status :: enabled | disabled | '_',
    desc :: binary() | '_',
    created_at :: emqx_datetime:epoch_second() | '_' | undefined,
    updated_at :: emqx_datetime:epoch_second() | '_' | undefined
}).

-define(TENANCY, emqx_tenancy).

%% type references

-type limiter_type() :: emqx_limiter_schema:limiter_type().
%% XXX: bucket_id ??
-type bucket_id() :: emqx_limiter_schema:limiter_id().

-endif.
