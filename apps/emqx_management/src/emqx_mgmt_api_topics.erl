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

-module(emqx_mgmt_api_topics).

-include_lib("emqx/include/emqx.hrl").
-include_lib("typerefl/include/types.hrl").

%% API
-behaviour(minirest_api).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1
]).

-export([
    topics/2,
    topic/2
]).

-export([query/4]).

-define(TOPIC_NOT_FOUND, 'TOPIC_NOT_FOUND').

-define(TOPICS_QUERY_SCHEMA, [
    {<<"topic">>, binary},
    {<<"node">>, atom},
    {<<"match_topic">>, binary}
]).
-define(TAGS, [<<"Topics">>]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    ["/topics", "/topics/:topic"].

schema("/topics") ->
    #{
        'operationId' => topics,
        get => #{
            description => <<"Topics list">>,
            tags => ?TAGS,
            parameters => [
                topic_param(query),
                hoconsc:ref(emqx_dashboard_swagger, tenant_id),
                node_param(),
                hoconsc:ref(emqx_dashboard_swagger, page),
                hoconsc:ref(emqx_dashboard_swagger, limit)
            ],
            responses => #{
                200 => [
                    {data, hoconsc:mk(hoconsc:array(hoconsc:ref(topic)), #{})},
                    {meta, hoconsc:mk(hoconsc:ref(emqx_dashboard_swagger, meta), #{})}
                ]
            }
        }
    };
schema("/topics/:topic") ->
    #{
        'operationId' => topic,
        get => #{
            description => <<"Lookup topic info by name">>,
            tags => ?TAGS,
            parameters => [
                hoconsc:ref(emqx_dashboard_swagger, tenant_id),
                topic_param(path)
            ],
            responses => #{
                200 => hoconsc:mk(hoconsc:ref(topic), #{}),
                404 =>
                    emqx_dashboard_swagger:error_codes(['TOPIC_NOT_FOUND'], <<"Topic not found">>)
            }
        }
    }.

fields(topic) ->
    [
        {topic,
            hoconsc:mk(binary(), #{
                desc => <<"Topic Name">>,
                required => true
            })},
        {node,
            hoconsc:mk(binary(), #{
                desc => <<"Node">>,
                required => true
            })}
    ].

%%%==============================================================================================
%% parameters trans
topics(get, #{query_string := Qs} = Req) ->
    TenantId = emqx_dashboard_utils:tenant(Req),
    do_list(trans_topic(Qs, TenantId), TenantId).

topic(get, #{bindings := Bindings} = Req) ->
    TenantId = emqx_dashboard_utils:tenant(Req),
    lookup(trans_topic(Bindings, TenantId), TenantId).

%%%==============================================================================================
%% api apply
do_list(Params, TenantId) ->
    case
        emqx_mgmt_api:node_query(
            node(), Params, emqx_route, ?TOPICS_QUERY_SCHEMA, {?MODULE, query}
        )
    of
        {error, page_limit_invalid} ->
            {400, #{code => <<"INVALID_PARAMETER">>, message => <<"page_limit_invalid">>}};
        {error, Node, {badrpc, R}} ->
            Message = list_to_binary(io_lib:format("bad rpc call ~p, Reason ~p", [Node, R])),
            {500, #{code => <<"NODE_DOWN">>, message => Message}};
        #{data := DataRaw} = Response ->
            Data = lists:map(
                fun(#{topic := Topic} = D) ->
                    D#{topic => emqx_mgmt_api_clients:unwarp_topic(Topic, TenantId)}
                end,
                DataRaw
            ),
            {200, Response#{data => Data}}
    end.

lookup(#{topic := Topic}, TenantId) ->
    case emqx_router:lookup_routes(Topic) of
        [] ->
            {404, #{code => ?TOPIC_NOT_FOUND, message => <<"Topic not found">>}};
        [Route] ->
            Response = #{topic := TopicRaw} = format(Route),
            {200, Response#{topic => emqx_mgmt_api_clients:unwarp_topic(TopicRaw, TenantId)}}
    end.

%%%==============================================================================================
%% internal
trans_topic(Params = #{<<"topic">> := Topic}, TenantId) ->
    Params#{
        <<"topic">> => emqx_mgmt_api_clients:warp_topic(uri_string:percent_decode(Topic), TenantId)
    };
trans_topic(Params = #{topic := Topic}, TenantId) ->
    Params#{topic => emqx_mgmt_api_clients:warp_topic(uri_string:percent_decode(Topic), TenantId)};
trans_topic(Params, TenantId) ->
    case emqx_mgmt_api_clients:warp_topic(<<"#">>, TenantId) of
        <<"#">> -> Params;
        WarpTopic -> Params#{<<"match_topic">> => WarpTopic}
    end.

query(Tab, {Qs, Fuzzy}, Continuation, Limit) ->
    Ms = qs2ms(Qs, [{{route, '_', '_'}, [], ['$_']}]),
    FuzzyFilterFun = fuzzy_filter_fun(Fuzzy),
    emqx_mgmt_api:select_table_with_count(
        Tab, {Ms, FuzzyFilterFun}, Continuation, Limit, fun format/1
    ).

qs2ms([], Res) ->
    Res;
qs2ms([{topic, '=:=', T} | Qs], [{{route, _, N}, [], ['$_']}]) ->
    qs2ms(Qs, [{{route, T, N}, [], ['$_']}]);
qs2ms([{node, '=:=', N} | Qs], [{{route, T, _}, [], ['$_']}]) ->
    qs2ms(Qs, [{{route, T, N}, [], ['$_']}]).

fuzzy_filter_fun(Fuzzy) ->
    fun(MsRaws) when is_list(MsRaws) ->
        lists:filter(
            fun(E) -> run_fuzzy_filter(E, Fuzzy) end,
            MsRaws
        )
    end.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(E = {route, Topic, _Node}, [{topic, match, TopicFilter} | Fuzzy]) ->
    emqx_topic:match(Topic, TopicFilter) andalso run_fuzzy_filter(E, Fuzzy).

format(#route{topic = Topic, dest = {_, Node}}) ->
    #{topic => Topic, node => Node};
format(#route{topic = Topic, dest = Node}) ->
    #{topic => Topic, node => Node}.

topic_param(In) ->
    {
        topic,
        hoconsc:mk(binary(), #{
            desc => <<"Topic Name">>,
            in => In,
            required => (In == path),
            example => <<"">>
        })
    }.

node_param() ->
    {
        node,
        hoconsc:mk(binary(), #{
            desc => <<"Node Name">>,
            in => query,
            required => false,
            example => node()
        })
    }.
