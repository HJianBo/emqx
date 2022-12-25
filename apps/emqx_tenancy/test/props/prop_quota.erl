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
            QuotaCfg = #{max_sessions => 0, max_authn_users => 0, max_authz_rules => 0},
            ok = emqx_tenancy_quota:create(TenantId, QuotaCfg),
            ?assertEqual(
                allow,
                emqx_hooks:run_fold('quota.sessions', [acquire, #{tenant_id => ?NO_TENANT}], allow)
            ),
            ?assertEqual(
                deny,
                emqx_hooks:run_fold('quota.sessions', [acquire, #{tenant_id => TenantId}], allow)
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
            ok = emqx_tenancy_quota:stop(),
            ok = emqx_tenancy_quota:unload(),
            true
        end
    ).

%%--------------------------------------------------------------------
%% setup & teardown

do_setup() ->
    net_kernel:start([?NODENAME, longnames]),
    ok = meck:new(emqx_tenancy_resm, [passthrough, no_history]),
    ok = meck:expect(
        emqx_tenancy_resm,
        monitor_session_proc,
        fun(_, _, _) -> ok end
    ),
    emqx_common_test_helpers:boot_modules(all),
    emqx_common_test_helpers:start_apps([]),
    ok.

do_teardown(_) ->
    emqx_common_test_helpers:stop_apps([]),
    ok = meck:unload(emqx_tenancy_resm).

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
        {call, emqx_tenancy_quota, on_quota_sessions, [quota_action(), clientinfo(), allow]},
        {call, emqx_tenancy_quota, on_quota_authn_users, [quota_action(), tenant_id(), allow]},
        {call, emqx_tenancy_quota, on_quota_authz_rules, [quota_action(), tenant_id(), allow]},
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
        max_authz_rules := MaxRules
    } = Conf,
    ?assertMatch(
        {ok, #{
            sessions := #{max := MaxSess},
            authn_users := #{max := MaxUsers},
            authz_rules := #{max := MaxRules}
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
            ?assertEqual(maps:get(authz_rules, Usage), maps:get(authz_rules, Return))
    end,
    true;
postcondition(State, {call, _Mod, on_quota_sessions, [Action, #{tenant_id := Id}, _]}, Res) ->
    %% assert
    {Wanted, _State} = apply_quota_action(Action, Id, sessions, State),
    ?assertMatch(Wanted, Res),
    %% await quota server sync changes to mnesia table
    await_changes_synced(Res),
    true;
postcondition(State, {call, _Mod, on_quota_authn_users, [Action, Id, _]}, Res) ->
    %% assert
    {Wanted, _State} = apply_quota_action(Action, Id, authn_users, State),
    ?assertMatch(Wanted, Res),
    %% await quota server sync changes to mnesia table
    await_changes_synced(Res),
    true;
postcondition(State, {call, _Mod, on_quota_authz_users, [Action, Id, _]}, Res) ->
    %% assert
    {Wanted, _State} = apply_quota_action(Action, Id, authz_rules, State),
    ?assertMatch(Wanted, Res),
    %% await quota server sync changes to mnesia table
    await_changes_synced(Res),
    true;
postcondition(_State, {call, _Mod, _Fun, _Args}, _Res) ->
    true.

await_changes_synced({_, allow}) ->
    {ok, _} = ?block_until(#{?snk_kind := submit}, 1000);
await_changes_synced(_) ->
    ok.

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
    case maps:get(TenantId, State, undefined) of
        undefined ->
            {{stop, deny}, State};
        Usage ->
            #{max := Max, used := Used} = maps:get(Resouse, Usage),
            case Max < N + Used of
                true ->
                    {{stop, deny}, State};
                false ->
                    NState = State#{TenantId => Usage#{Resouse => #{max => Max, used => N + Used}}},
                    {{stop, allow}, NState}
            end
    end.

config_to_usage(
    #{max_sessions := MaxSess, max_authn_users := MaxAuthn, max_authz_rules := MaxAuthz},
    Old
) ->
    UsedSess = emqx_map_lib:deep_get([sessions, used], Old, 0),
    UsedAuthn = emqx_map_lib:deep_get([authn_users, used], Old, 0),
    UsedAuthz = emqx_map_lib:deep_get([authz_rules, used], Old, 0),
    #{
        sessions => #{max => MaxSess, used => UsedSess},
        authn_users => #{max => MaxAuthn, used => UsedAuthn},
        authz_rules => #{max => MaxAuthz, used => UsedAuthz}
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
        {A, B, C},
        {pos_integer(), pos_integer(), pos_integer()},
        begin
            #{max_sessions => A, max_authn_users => B, max_authz_rules => C}
        end
    ).

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
