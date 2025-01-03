%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_tt_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-import(hoconsc, [
    mk/2,
    array/1,
    enum/1,
    ref/1
]).

-import(emqx_dashboard_sso_api, [login_meta/3]).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
]).

-export([callback/2]).

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(BAD_USERNAME_OR_PWD, 'BAD_USERNAME_OR_PWD').
-define(BACKEND_NOT_FOUND, 'BACKEND_NOT_FOUND').

-define(RESPHEADERS, #{
    <<"cache-control">> => <<"no-cache">>,
    <<"pragma">> => <<"no-cache">>,
    <<"content-type">> => <<"text/plain">>
}).
-define(REDIRECT_BODY, <<"Redirecting...">>).

-define(TAGS, <<"Dashboard Single Sign-On">>).
-define(BACKEND, tongauth).
-define(BASE_PATH, "/api/v5").
-define(CALLBACK_PATH, "/sso/tongauth/callback").

namespace() -> "dashboard_sso".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false, translate_body => false}).

paths() ->
    [
        ?CALLBACK_PATH
    ].

schema("/sso/tongauth/callback") ->
    #{
        'operationId' => callback,
        get => #{
            tags => [?TAGS],
            responses => #{
                302 => response_schema(302),
                400 => response_schema(400),
                401 => response_schema(401),
                404 => response_schema(404)
            },
            security => [],
            log_meta => emqx_dashboard_audit:importance(high)
        }
    }.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

callback(get, #{query_string := QS}) ->
    minirest_handler:update_log_meta(#{log_from => tongauth}),
    case check_and_retrieve_token(QS) of
        {ok, Target} ->
            ?SLOG(info, #{
                msg => "dashboard_sso_login_successful"
            }),
            {302, ?RESPHEADERS#{<<"location">> => Target}, ?REDIRECT_BODY};
        {error, invalid_query_string_param} ->
            {400, #{code => ?BAD_REQUEST, message => <<"Invalid query string">>}};
        {error, invalid_backend} ->
            {404, #{code => ?BACKEND_NOT_FOUND, message => <<"Backend not found">>}};
        {error, Reason} ->
            ?SLOG(info, #{
                msg => "dashboard_sso_login_failed",
                reason => emqx_utils:redact(Reason)
            }),
            {401, #{code => ?BAD_USERNAME_OR_PWD, message => reason_to_message(Reason)}}
    end.

%%--------------------------------------------------------------------
%% internal
%%--------------------------------------------------------------------

response_schema(302) ->
    emqx_dashboard_swagger:error_codes(['REDIRECT'], <<"Redirect">>);
response_schema(400) ->
    emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad Request">>);
response_schema(401) ->
    emqx_dashboard_swagger:error_codes([?BAD_USERNAME_OR_PWD], <<"Login failed">>);
response_schema(404) ->
    emqx_dashboard_swagger:error_codes([?BACKEND_NOT_FOUND], <<"Backend not found">>).

check_and_retrieve_token(QS) ->
    case emqx_dashboard_sso_manager:lookup_state(?BACKEND) of
        undefined ->
            {error, invalid_backend};
        Cfg ->
            emqx_dashboard_sso_tt:callback(QS, Cfg)
    end.

reason_to_message(Bin) when is_binary(Bin) ->
    Bin;
reason_to_message(Term) ->
    erlang:iolist_to_binary(io_lib:format("~p", [Term])).
