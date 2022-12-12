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

-type tenant_id() :: binary().

-record(tenant, {
    id :: tenant_id() | '_' | undefined,
    quota :: map() | '_' | undefined,
    status :: enabled | disabled | '_',
    desc :: binary() | '_',
    created_at :: emqx_datetime:epoch_second() | '_' | undefined,
    updated_at :: emqx_datetime:epoch_second() | '_' | undefined
}).

-record(tenant_usage, {sni, usage}).

-define(TENANCY, emqx_tenancy).

-endif.
