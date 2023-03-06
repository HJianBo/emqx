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

-module(emqx_retainer_api).

-behaviour(minirest_api).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% API
-export([api_spec/0, paths/0, schema/1, namespace/0, fields/1]).

-export([
    lookup_retained_warp/2,
    with_topic_warp/2,
    config/2
]).

-import(hoconsc, [mk/1, mk/2, ref/1, ref/2, array/1]).
-import(emqx_dashboard_swagger, [error_codes/2]).

%% 1MB = 1024 x 1024
-define(MAX_PAYLOAD_SIZE, 1048576).
-define(PREFIX, "/mqtt/retainer").
-define(TAGS, [<<"retainer">>]).

namespace() -> "retainer".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [?PREFIX, ?PREFIX ++ "/messages", ?PREFIX ++ "/message/:topic"].

schema(?PREFIX) ->
    #{
        'operationId' => config,
        get => #{
            tags => ?TAGS,
            description => ?DESC(get_config_api),
            responses => #{
                200 => mk(conf_schema(), #{desc => ?DESC(config_content)}),
                404 => error_codes(['NOT_FOUND'], ?DESC(config_not_found))
            }
        },
        put => #{
            tags => ?TAGS,
            description => ?DESC(update_retainer_api),
            'requestBody' => mk(conf_schema(), #{desc => ?DESC(config_content)}),
            responses => #{
                200 => mk(conf_schema(), #{desc => ?DESC(update_config_success)}),
                400 => error_codes(['UPDATE_FAILED'], ?DESC(update_config_failed))
            }
        }
    };
schema(?PREFIX ++ "/messages") ->
    #{
        'operationId' => lookup_retained_warp,
        get => #{
            tags => ?TAGS,
            description => ?DESC(list_retained_api),
            parameters => page_params(),
            responses => #{
                200 => [
                    {data, mk(array(ref(message_summary)), #{desc => ?DESC(retained_list)})},
                    {meta, mk(hoconsc:ref(emqx_dashboard_swagger, meta))}
                ],
                400 => error_codes(['BAD_REQUEST'], ?DESC(unsupported_backend))
            }
        }
    };
schema(?PREFIX ++ "/message/:topic") ->
    #{
        'operationId' => with_topic_warp,
        get => #{
            tags => ?TAGS,
            description => ?DESC(lookup_api),
            parameters => parameters(),
            responses => #{
                200 => mk(ref(message), #{desc => ?DESC(message_detail)}),
                404 => error_codes(['NOT_FOUND'], ?DESC(message_not_exist)),
                400 => error_codes(['BAD_REQUEST'], ?DESC(unsupported_backend))
            }
        },
        delete => #{
            tags => ?TAGS,
            description => ?DESC(delete_matching_api),
            parameters => parameters(),
            responses => #{
                204 => <<>>,
                400 => error_codes(
                    ['BAD_REQUEST'],
                    ?DESC(unsupported_backend)
                )
            }
        }
    }.

page_params() ->
    [hoconsc:ref(emqx_dashboard_swagger, tenant_id)] ++
        emqx_dashboard_swagger:fields(page) ++ emqx_dashboard_swagger:fields(limit).

conf_schema() ->
    ref(emqx_retainer_schema, "retainer").

parameters() ->
    [
        {topic,
            mk(binary(), #{
                in => path,
                required => true,
                desc => ?DESC(topic)
            })},
        hoconsc:ref(emqx_dashboard_swagger, tenant_id)
    ].

fields(message_summary) ->
    [
        {msgid, mk(binary(), #{desc => ?DESC(msgid)})},
        {topic, mk(binary(), #{desc => ?DESC(topic)})},
        {qos, mk(emqx_schema:qos(), #{desc => ?DESC(qos)})},
        {publish_at, mk(string(), #{desc => ?DESC(publish_at)})},
        {from_clientid, mk(binary(), #{desc => ?DESC(from_clientid)})},
        {from_username, mk(binary(), #{desc => ?DESC(from_username)})}
    ];
fields(message) ->
    [
        {payload, mk(binary(), #{desc => ?DESC(payload)})}
        | fields(message_summary)
    ].

lookup_retained_warp(Type, Params) ->
    check_backend(Type, Params, fun lookup_retained/2).

with_topic_warp(Type, Params) ->
    check_backend(Type, Params, fun with_topic/2).

config(get, _) ->
    {200, emqx:get_raw_config([retainer])};
config(put, #{body := Body}) ->
    try
        {ok, _} = emqx_retainer:update_config(Body),
        {200, emqx:get_raw_config([retainer])}
    catch
        _:Reason:_ ->
            {400, #{
                code => <<"UPDATE_FAILED">>,
                message => iolist_to_binary(io_lib:format("~p~n", [Reason]))
            }}
    end.

%%------------------------------------------------------------------------------
%% Interval Funcs
%%------------------------------------------------------------------------------

lookup_retained(get, Params = #{query_string := Qs}) ->
    Page = maps:get(<<"page">>, Qs, 1),
    Limit = maps:get(<<"limit">>, Qs, emqx_mgmt:max_row_limit()),
    TopicPrefix = extract_topic_prefix(Params),
    TopicQuery =
        case TopicPrefix of
            undefined -> undefined;
            _ -> <<TopicPrefix/binary, "#">>
        end,
    {ok, Msgs, Total} = emqx_retainer_mnesia:page_read(undefined, TopicQuery, Page, Limit),
    {200, #{
        data => [format_message(TopicPrefix, Msg) || Msg <- Msgs],
        meta => #{page => Page, limit => Limit, count => Total}
    }}.

with_topic(get, Params = #{bindings := Bindings}) ->
    Prefix = extract_topic_prefix(Params, <<>>),
    Topic0 = maps:get(topic, Bindings),
    Topic = <<Prefix/binary, Topic0/binary>>,
    {ok, Msgs, _Total} = emqx_retainer_mnesia:page_read(undefined, Topic, 1, 1),
    case Msgs of
        [H | _] ->
            {200, format_detail_message(Prefix, H)};
        _ ->
            {404, #{
                code => <<"NOT_FOUND">>,
                message => <<"Viewed message doesn't exist">>
            }}
    end;
with_topic(delete, Params = #{bindings := Bindings}) ->
    Prefix = extract_topic_prefix(Params, <<>>),
    Topic0 = maps:get(topic, Bindings),
    Topic = <<Prefix/binary, Topic0/binary>>,
    emqx_retainer_mnesia:delete_message(undefined, Topic),
    {204}.

extract_topic_prefix(Params) ->
    extract_topic_prefix(Params, undefined).
extract_topic_prefix(Params, Default) ->
    case emqx_dashboard_utils:tenant(Params) of
        ?NO_TENANT ->
            Default;
        Id ->
            emqx_mountpoint:replvar(?TENANT_TOPIC_PREFIX, #{tenant_id => Id})
    end.

format_message(Prefix, #message{
    id = ID,
    qos = Qos,
    topic = Topic,
    from = From,
    timestamp = Timestamp,
    headers = Headers
}) ->
    #{
        msgid => emqx_guid:to_hexstr(ID),
        qos => Qos,
        topic => emqx_mountpoint:unmount(Prefix, Topic),
        publish_at => list_to_binary(
            calendar:system_time_to_rfc3339(
                Timestamp, [{unit, millisecond}]
            )
        ),
        from_clientid => to_bin_string(emqx_clientid:without_tenant(From)),
        from_username => maps:get(username, Headers, <<>>)
    }.

format_detail_message(Prefix, #message{payload = Payload} = Msg) ->
    Base = format_message(Prefix, Msg),
    case erlang:byte_size(Payload) =< ?MAX_PAYLOAD_SIZE of
        true ->
            Base#{payload => base64:encode(Payload)};
        _ ->
            Base
    end.

to_bin_string(Data) when is_binary(Data) ->
    Data;
to_bin_string(Data) ->
    list_to_binary(io_lib:format("~p", [Data])).

check_backend(Type, Params, Cont) ->
    case emqx:get_config([retainer, backend, type]) of
        built_in_database ->
            Cont(Type, Params);
        _ ->
            {400, 'BAD_REQUEST', <<"This API only support built in database">>}
    end.
