%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_tt).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(emqx_dashboard_sso).

-export([
    namespace/0,
    hocon_ref/0,
    login_ref/0,
    fields/1,
    desc/1
]).

%% emqx_dashboard_sso callbacks
-export([
    create/1,
    update/2,
    destroy/1,
    convert_certs/2,
    login/2
]).

-export([
    callback/2
]).

-define(BACKEND, tongauth).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() -> "dashboard".

hocon_ref() ->
    hoconsc:ref(?MODULE, tongauth).

login_ref() ->
    hoconsc:ref(?MODULE, login).

fields(tongauth) ->
    emqx_dashboard_sso_schema:common_backend_schema([tongauth]) ++
        [
            {dashboard_addr, fun dashboard_addr/1},
            {auth_server_addr, fun auth_server_addr/1},
            {client_id, fun client_id/1},
            {client_secret, fun client_secret/1}
        ];
fields(login) ->
    [
        emqx_dashboard_sso_schema:backend_schema([tongauth])
    ].

dashboard_addr(type) -> binary();
dashboard_addr(desc) -> ?DESC(dashboard_addr);
dashboard_addr(default) -> <<"https://127.0.0.1:18083">>;
dashboard_addr(_) -> undefined.

auth_server_addr(type) -> binary();
auth_server_addr(desc) -> ?DESC(auth_server_addr);
auth_server_addr(default) -> <<"https://tongauth.emqx.io">>;
auth_server_addr(_) -> undefined.

client_id(type) -> binary();
client_id(desc) -> ?DESC(client_id);
client_id(default) -> <<"1234567890">>;
client_id(_) -> undefined.

client_secret(type) -> binary();
client_secret(desc) -> ?DESC(client_secret);
client_secret(default) -> <<"1234567890">>;
client_secret(_) -> undefined.

desc(tongauth) ->
    "tongauth";
desc(_) ->
    undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec create(emqx_dashboard_sso:config()) ->
    {ok, emqx_dashboard_sso:state()} | {error, term()}.
create(#{enable := false} = _Config) ->
    {ok, undefined};
create(
    #{
        dashboard_addr := DashboardAddr,
        auth_server_addr := AuthServerAddr,
        client_id := ClientId,
        client_secret := ClientSecret
    } = _Config
) ->
    {ok, #{
        dashboard_addr => DashboardAddr,
        auth_server_addr => AuthServerAddr,
        client_id => ClientId,
        client_secret => ClientSecret
    }}.

-spec update(emqx_dashboard_sso:config(), emqx_dashboard_sso:state()) ->
    {ok, emqx_dashboard_sso:state()} | {error, term()}.
update(Config0, State) ->
    destroy(State),
    create(Config0).

-spec destroy(emqx_dashboard_sso:state()) -> ok.
destroy(_State) ->
    ok.

-spec login(emqx_dashboard_sso:request(), emqx_dashboard_sso:state()) ->
    {redirect, tuple()}.
login(_Req, State) ->
    {redirect, make_login_redirect_to_tongauth_url(State)}.

convert_certs(_Dir, Conf) ->
    Conf.

-spec callback(map(), map()) -> {ok, binary()} | {error, invalid_query_string_param | term()}.
callback(
    #{<<"code">> := Code},
    State
) ->
    case retrieve_token(Code, State) of
        {ok, Token} ->
            retrieve_userinfo(Token, State);
        Error ->
            Error
    end;
callback(_, _) ->
    {error, invalid_query_string_param}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

retrieve_token(Code, State) ->
    Url = make_retrieve_token_url(State),
    Body = make_retrieve_token_body(Code, State),
    Headers = [{"accept", "application/json"}],
    ContentType = <<"application/x-www-form-urlencoded">>,
    case httpc:request(post, {Url, Headers, ContentType, Body}, [], []) of
        {ok, {{_, 200, _}, _, Resp}} ->
            case emqx_utils_json:decode(Resp, [return_maps]) of
                #{<<"access_token">> := Token} ->
                    {ok, Token};
                _Unkown ->
                    {error, {unknown_response, Resp}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

retrieve_userinfo(Token, State) ->
    Url = make_retrieve_userinfo_url(State),
    Headers = [
        {"accept", "application/json"},
        {"Authorization", binary_to_list(fmt("Bearer ~s", [Token]))}
    ],
    case httpc:request(get, {Url, Headers}, [], []) of
        {ok, {{_, 200, _}, _, Resp}} ->
            case emqx_utils_json:decode(Resp, [return_maps]) of
                #{<<"data">> := #{<<"username">> := Username}} ->
                    ?SLOG(debug, #{
                        msg => "sso_tt_login_user_info",
                        username => Username
                    }),
                    minirest_handler:update_log_meta(#{log_source => Username}),
                    ensure_user_exists(State, Username);
                _Unkown ->
                    {error, {unknown_response, Resp}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

ensure_user_exists(_Cfg, <<>>) ->
    {error, <<"Username can not be empty">>};
ensure_user_exists(_Cfg, <<"undefined">>) ->
    {error, <<"Username can not be undefined">>};
ensure_user_exists(Cfg, Username) ->
    case emqx_dashboard_admin:lookup_user(?BACKEND, Username) of
        [User] ->
            case emqx_dashboard_token:sign(User, <<>>) of
                {ok, Role, Token} ->
                    {ok, login_redirect_target(Cfg, Username, Role, Token)};
                Error ->
                    Error
            end;
        [] ->
            case emqx_dashboard_admin:add_sso_user(?BACKEND, Username, ?ROLE_VIEWER, <<>>) of
                {ok, _} ->
                    ensure_user_exists(Cfg, Username);
                Error ->
                    Error
            end
    end.

login_redirect_target(#{config := #{dashboard_addr := Addr}}, Username, Role, Token) ->
    LoginMeta = emqx_dashboard_sso_api:login_meta(Username, Role, Token, oidc),
    MetaBin = base64:encode(emqx_utils_json:encode(LoginMeta)),
    <<Addr/binary, "/?login_meta=", MetaBin/binary>>.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

make_login_redirect_to_tongauth_url(
    #{
        dashboard_addr := DashboardAddr,
        auth_server_addr := AuthServerAddr,
        client_id := ClientId
    }
) ->
    CallbackUrl = fmt("~s/api/v5/sso/tongauth/callback", [DashboardAddr]),
    fmt("~s/tongauth/authorize?client_id=~s&redirect_uri=~s&response_type=code", [
        AuthServerAddr, ClientId, CallbackUrl
    ]).

make_retrieve_token_url(#{auth_server_addr := AuthServerAddr}) ->
    fmt("~s/tongauth/token", [AuthServerAddr]).

make_retrieve_token_body(Code, #{client_id := ClientId, client_secret := ClientSecret}) ->
    cow_qs:qs(
        [
            {<<"client_id">>, ClientId},
            {<<"client_secret">>, ClientSecret},
            {<<"code">>, Code}
        ]
    ).

make_retrieve_userinfo_url(#{auth_server_addr := AuthServerAddr, client_id := ClientId}) ->
    fmt("~s/tongauth/user?client_id=~s", [AuthServerAddr, ClientId]).

fmt(Fmt, Args) ->
    list_to_binary(io_lib:format(Fmt, Args)).
