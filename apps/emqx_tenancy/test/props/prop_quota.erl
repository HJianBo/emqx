-module(prop_quota).

-include_lib("emqx/include/emqx.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(ALL(Vars, Types, Exprs),
    ?SETUP(
        fun() ->
            State = do_setup(),
            fun() -> do_teardown(State) end
        end,
        ?FORALL(Vars, Types, Exprs)
    )
).

-define(NODENAME, 'proper@127.0.0.1').

%% Model Callbacks
-export([
    command/1,
    initial_state/0,
    next_state/3,
    precondition/2,
    postcondition/3
]).

%%--------------------------------------------------------------------
%% properties
%%--------------------------------------------------------------------

prop_clear_start() ->
    ?ALL(
        Cmds,
        commands(?MODULE),
        begin
            %% clear table
            mnesia:clear_table(emqx_tenancy_usage),
            ok = snabbkaffe:start_trace(),

            {ok, _Pid} = emqx_tenancy_quota:start_link(#{
                delayed_submit_ms => 0, delayed_submit_diff => 0
            }),
            {History, State, Result} = run_commands(?MODULE, Cmds),

            emqx_tenancy_quota:stop(),
            mnesia:clear_table(emqx_tenancy_quota_counter),
            snabbkaffe:stop(),

            ?WHENFAIL(
                io:format("History: ~p\nState: ~p\nResult: ~p\n", [History, State, Result]),
                aggregate(command_names(Cmds), Result =:= ok)
            )
        end
    ).

%% XXX: it's just a common cases
prop_load_unload() ->
    ?ALL(
        _,
        pos_integer(),
        begin
            ok = emqx_tenancy_quota:load(),

            {ok, _Pid} = emqx_tenancy_quota:start_link(#{}),
            TenantId = <<"tenant_foo">>,
            QuotaCfg = #{
                max_sessions => 0,
                max_authn_users => 0,
                max_authz_rules => 0,
                max_retained_msgs => 0
            },
            ok = emqx_tenancy_quota:create(TenantId, QuotaCfg),
            ?assertEqual(
                allow,
                emqx_hooks:run_fold('quota.sessions', [acquire, #{tenant_id => ?NO_TENANT}], allow)
            ),
            ?assertEqual(
                deny,
                emqx_hooks:run_fold(
                    'quota.sessions', [acquire, #{tenant_id => TenantId, clientid => <<>>}], allow
                )
            ),

            ?assertEqual(
                allow,
                emqx_hooks:run_fold('quota.authn_users', [acquire, ?NO_TENANT], allow)
            ),
            ?assertEqual(
                deny,
                emqx_hooks:run_fold('quota.authn_users', [acquire, TenantId], allow)
            ),

            ?assertEqual(
                allow,
                emqx_hooks:run_fold('quota.authz_rules', [acquire, ?NO_TENANT], allow)
            ),
            ?assertEqual(
                deny,
                emqx_hooks:run_fold('quota.authz_rules', [acquire, TenantId], allow)
            ),

            ?assertEqual(
                allow,
                emqx_hooks:run_fold('quota.retained_msgs', [acquire, ?NO_TENANT], allow)
            ),
            ?assertEqual(
                deny,
                emqx_hooks:run_fold('quota.retained_msgs', [acquire, TenantId], allow)
            ),

            ok = emqx_tenancy_quota:stop(),
            mnesia:clear_table(emqx_tenancy_quota_counter),
            ok = emqx_tenancy_quota:unload(),
            true
        end
    ).

%%--------------------------------------------------------------------
%% setup & teardown

-define(BASE_CONF, <<
    ""
    "\n"
    "retainer {\n"
    "    enable = true\n"
    "    msg_clear_interval = 0s\n"
    "    msg_expiry_interval = 0s\n"
    "    max_payload_size = 1MB\n"
    "    flow_control {\n"
    "        batch_read_number = 0\n"
    "        batch_deliver_number = 0\n"
    "     }\n"
    "   backend {\n"
    "        type = built_in_database\n"
    "        storage_type = ram\n"
    "        max_retained_messages = 0\n"
    "     }\n"
    "}"
    ""
>>).

do_setup() ->
    net_kernel:start([?NODENAME, longnames]),
    ok = meck:new(emqx_tenancy_resm, [passthrough, no_history]),
    ok = meck:new(emqx_tenancy_quota, [passthrough, no_history]),
    ok = meck:new(emqx_cm, [passthrough, no_history]),
    ok = meck:expect(
        emqx_tenancy_resm,
        monitor_session_proc,
        fun(_, _, _) -> ok end
    ),
    ok = meck:expect(
        emqx_tenancy_quota,
        is_tenant_enabled,
        fun(_) -> true end
    ),
    ok = meck:expect(
        emqx_cm,
        lookup_channels,
        fun(_) -> [] end
    ),
    ok = emqx_config:init_load(emqx_retainer_schema, ?BASE_CONF),
    emqx_common_test_helpers:boot_modules(all),
    emqx_common_test_helpers:start_apps([emqx_retainer]),
    ok.

do_teardown(_) ->
    emqx_common_test_helpers:stop_apps([emqx_retainer]),
    _ = meck:unload(),
    ok = emqx_config:delete_override_conf_files(),
    ok.

%%--------------------------------------------------------------------
%% model
%%--------------------------------------------------------------------

%% @doc Initial model value at system start. Should be deterministic.
initial_state() ->
    #{}.

%% @doc List of possible commands to run against the system
command(_State) ->
    oneof([
        %% apis
        {call, emqx_tenancy_quota, create, [tenant_id(), quota_config()]},
        {call, emqx_tenancy_quota, update, [tenant_id(), quota_config()]},
        {call, emqx_tenancy_quota, remove, [tenant_id()]},
        {call, emqx_tenancy_quota, info, [tenant_id()]},
        {call, emqx_tenancy_quota, on_quota_sessions, [quota_action_name(), clientinfo(), allow]},
        {call, emqx_tenancy_quota, on_quota_authn_users, [quota_action(), tenant_id(), allow]},
        {call, emqx_tenancy_quota, on_quota_authz_rules, [quota_action(), tenant_id(), allow]},
        {call, emqx_tenancy_quota, on_quota_retained_msgs, [quota_action(), tenant_id(), allow]},

        %% misc
        {call, emqx_tenancy_quota, load, []},
        {call, emqx_tenancy_quota, unload, []}
    ]).

%% @doc Determines whether a command should be valid under the
%% current state.
precondition(_State, {call, _Mod, _Fun, _Args}) ->
    true.

%% @doc Given the state `State' *prior* to the call
%% `{call, Mod, Fun, Args}', determine whether the result
%% `Res' (coming from the actual system) makes sense.
postcondition(_State, {call, _Mod, CreateOrUpdate, [Id, Conf]}, Res) when
    CreateOrUpdate =:= create;
    CreateOrUpdate =:= update
->
    %% assert: create/update always return ok
    ?assertMatch(ok, Res),
    %% assert max value
    #{
        max_sessions := MaxSess,
        max_authn_users := MaxUsers,
        max_authz_rules := MaxRules,
        max_retained_msgs := MaxRetained
    } = Conf,
    ?assertMatch(
        {ok, #{
            sessions := #{max := MaxSess},
            authn_users := #{max := MaxUsers},
            authz_rules := #{max := MaxRules},
            retained_msgs := #{max := MaxRetained}
        }},
        emqx_tenancy_quota:do_info(Id)
    ),
    true;
postcondition(_State, {call, _Mod, remove, [Id]}, Res) ->
    %% assert: remove always return ok
    ?assertMatch(ok, Res),
    %% assert: can't get usage_info
    ?assertMatch(
        {error, not_found},
        emqx_tenancy_quota:do_info(Id)
    ),
    true;
postcondition(State, {call, _Mod, info, [Id]}, Res) ->
    case maps:get(Id, State, undefined) of
        undefined ->
            ?assertMatch({error, not_found}, Res);
        Usage ->
            {ok, Return} = Res,
            ?assertEqual(maps:get(sessions, Usage), maps:get(sessions, Return)),
            ?assertEqual(maps:get(authn_users, Usage), maps:get(authn_users, Return)),
            ?assertEqual(maps:get(authz_rules, Usage), maps:get(authz_rules, Return)),
            ?assertEqual(maps:get(retained_msgs, Usage), maps:get(retained_msgs, Return))
    end,
    true;
postcondition(State, {call, _Mod, on_quota_sessions, [Action, #{tenant_id := Id}, _]}, Res) ->
    %% assert
    {Wanted, _State} = apply_quota_action(Action, Id, sessions, State),
    ?assertMatch(Wanted, Res),
    true;
postcondition(State, {call, _Mod, on_quota_authn_users, [Action, Id, _]}, Res) ->
    %% assert
    {Wanted, _State} = apply_quota_action(Action, Id, authn_users, State),
    ?assertMatch(Wanted, Res),
    true;
postcondition(State, {call, _Mod, on_quota_authz_users, [Action, Id, _]}, Res) ->
    %% assert
    {Wanted, _State} = apply_quota_action(Action, Id, authz_rules, State),
    ?assertMatch(Wanted, Res),
    true;
postcondition(State, {call, _Mod, on_quota_retained_msgs, [Action, Id, _]}, Res) ->
    %% assert
    {Wanted, _State} = apply_quota_action(Action, Id, retained_msgs, State),
    ?assertMatch(Wanted, Res),
    true;
postcondition(_State, {call, _Mod, _Fun, _Args}, _Res) ->
    true.

%% @doc Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(State, {var, _}, {call, _Mod, _Fun, _Args}) ->
    %% ignore abstract calling
    State;
next_state(State, ok, {call, _Mod, CreateOrUpdate, [Id, Conf]}) when
    CreateOrUpdate =:= create;
    CreateOrUpdate =:= update
->
    State#{Id => config_to_usage(Conf, maps:get(Id, State, #{}))};
next_state(State, ok, {call, _Mod, remove, [Id]}) ->
    maps:remove(Id, State);
next_state(State, _Res, {call, _Mod, on_quota_sessions, [Action, #{tenant_id := Id}, _]}) ->
    {_, NState} = apply_quota_action(Action, Id, sessions, State),
    NState;
next_state(State, _Res, {call, _Mod, on_quota_authn_users, [Action, Id, _]}) ->
    {_, NState} = apply_quota_action(Action, Id, authn_users, State),
    NState;
next_state(State, _Res, {call, _Mod, on_quota_authz_rules, [Action, Id, _]}) ->
    {_, NState} = apply_quota_action(Action, Id, authz_rules, State),
    NState;
next_state(State, _Res, {call, _Mod, on_quota_retained_msgs, [Action, Id, _]}) ->
    {_, NState} = apply_quota_action(Action, Id, retained_msgs, State),
    NState;
next_state(State, _Res, {call, _Mod, _Fun, _Args}) ->
    State.

apply_quota_action(acquire, TenantId, Resouse, State) ->
    apply_quota_action(1, TenantId, Resouse, State);
apply_quota_action(release, TenantId, Resouse, State) ->
    apply_quota_action(-1, TenantId, Resouse, State);
apply_quota_action({acquire, N}, TenantId, Resouse, State) ->
    apply_quota_action(N, TenantId, Resouse, State);
apply_quota_action({release, N}, TenantId, Resouse, State) ->
    apply_quota_action(-N, TenantId, Resouse, State);
apply_quota_action(N, TenantId, Resouse, State) ->
    {Res, NState} =
        case maps:get(TenantId, State, undefined) of
            undefined ->
                {{stop, deny}, State};
            Usage ->
                #{max := Max, used := Used} = maps:get(Resouse, Usage),
                case Max < N + Used of
                    true ->
                        {{stop, deny}, State};
                    false ->
                        State1 = State#{
                            TenantId => Usage#{Resouse => #{max => Max, used => counting(Used, N)}}
                        },
                        {{stop, allow}, State1}
                end
        end,
    case N < 0 of
        true -> {{stop, allow}, NState};
        false -> {Res, NState}
    end.

counting(Used, N) ->
    Used1 = Used + N,
    case Used1 < 0 of
        true -> 0;
        false -> Used1
    end.

config_to_usage(
    #{
        max_sessions := MaxSess,
        max_authn_users := MaxAuthn,
        max_authz_rules := MaxAuthz,
        max_retained_msgs := MaxRetained
    },
    Old
) ->
    UsedSess = emqx_map_lib:deep_get([sessions, used], Old, 0),
    UsedAuthn = emqx_map_lib:deep_get([authn_users, used], Old, 0),
    UsedAuthz = emqx_map_lib:deep_get([authz_rules, used], Old, 0),
    UsedRetained = emqx_map_lib:deep_get([retained_msgs, used], Old, 0),
    #{
        sessions => #{max => MaxSess, used => UsedSess},
        authn_users => #{max => MaxAuthn, used => UsedAuthn},
        authz_rules => #{max => MaxAuthz, used => UsedAuthz},
        retained_msgs => #{max => MaxRetained, used => UsedRetained}
    }.

%%--------------------------------------------------------------------
%% generators
%%--------------------------------------------------------------------

tenant_id() ->
    oneof([<<"tenant_foo">>, <<"tennat_bar">>, rand_tenant_id()]).

rand_tenant_id() ->
    ?LET(
        Str,
        ?SUCHTHAT(S, list(oneof([integer($a, $z), integer($A, $Z), $_])), S =/= []),
        list_to_binary(Str)
    ).

quota_config() ->
    ?LET(
        {A, B, C, D},
        {pos_integer(), pos_integer(), pos_integer(), pos_integer()},
        begin
            #{max_sessions => A, max_authn_users => B, max_authz_rules => C, max_retained_msgs => D}
        end
    ).

quota_action_name() ->
    oneof([acquire, release]).

quota_action() ->
    oneof([acquire, release, {acquire, pos_integer()}, {release, pos_integer()}]).

clientinfo() ->
    ?LET(
        {ClientInfo, TenantId},
        {emqx_proper_types:clientinfo(), tenant_id()},
        begin
            ClientInfo#{
                tenant_id => TenantId,
                clientid => emqx_clientid:with_tenant(TenantId, maps:get(clientid, ClientInfo))
            }
        end
    ).
