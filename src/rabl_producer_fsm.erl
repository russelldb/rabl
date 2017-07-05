%%%-------------------------------------------------------------------
%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%
%%% And fsm that wraps a connection to rabbitmq for a
%%% producer. Borrowed liberally from basho/sidejob. Rabl use rabbitmq
%%% as a queue for replicating riak_objects. Rabbitmq erlang client
%%% connections are a process. Sending a message to rabbitmq is a cast
%%% to a process. Rabl uses Riak's post_commit hook to send a message
%%% to rabbitmq. The post_commit hook runs under the process of the
%%% put_fsm. There are a large number (potentially unbounded) numer of
%%% put_fsm processes. Rabbitmq does not like a large number of
%%% connections. It especially does not like a large number of
%%% connections to be attempted to be opened simulataneously. To that
%%% end rabl_producer_sup creates N workers (configured by {rabl,
%%% producer_count}) and each of those workers holds a connection to%
%%% rabbitmq. Now when the put_fsm needs a rabbitmq connection it
%%% could ask the supervisor for a child, but that still means a
%%% message to the supervisor per-put. This may lead to large message
%%% queues on% the sup. Instead, if we know the names of the workers,
%%% and the number of them in advance, we can pick one without a
%%% message send, and call it. If the worker is down, it doesn't
%%% matter, that is a dropped replication message, and fullsync will
%%% pick it up. Or the hook can try another worker. This may all be
%%% way over engineered. We use an fsm since we never want to crash
%%% the producer supervisor, when a producer_fsm has no rabbitmq
%%% connection, and it has been alive for a very short period of time,
%%% it does not die at once, instead it waits a delay. During that
%%% period it is in the `disconnected' state, and rejects attempts to
%%% publish rabbitmq messages.
%%
%%% @end
%%% Created :  3 Jul 2017 by Russell Brown <russell@wombat.me>
%%%-------------------------------------------------------------------
-module(rabl_producer_fsm).

-behaviour(gen_fsm).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/3, publish/1,
         load_producer/0, get_worker/0, producer_specs/0]).

%% gen_fsm callbacks
-export([init/1, connected/3,
         disconnected/3,
         handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-define(GENERATED_MOD, rabl_producer_gen).
%% @TOD(rdb|refactor) move to common header file
-define(DEFAULT_PRODUCER_COUNT, 10).
-define(DEFAULT_RECONN_DELAY_MILLIS, 50).

-record(state, {
          channel,
          queue,
          amqp_params,
          reconnect_delay_millis=?DEFAULT_RECONN_DELAY_MILLIS,
          start_time_millis=rabl_time:monotonic_time()
         }).

-ignore_xref({?GENERATED_MOD, workers, 0}).
-ignore_xref({?GENERATED_MOD, count, 0}).
-ignore_xref({?GENERATED_MOD, specs, 0}).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
-spec start_link(Name::atom(),
                 AMQPURI::string(),
                 Opts::list()) ->
                        {ok, Pid::pid()}
                            | ignore
                            | {error, Error::term()}.
start_link(Name, AMQPURI, Opts) ->
    gen_fsm:start_link({local, Name}, ?MODULE, [AMQPURI, Opts], []).

%%--------------------------------------------------------------------
%% @doc
%% Publish a replication message
%% @end
%%--------------------------------------------------------------------
-spec publish(Msg::binary()) -> ok.
publish(Msg) ->
    Worker = get_worker(),
    lager:debug("publishing on worker ~p~n", [Worker]),
    try
        gen_fsm:sync_send_event(Worker, {publish, Msg}, 1000)
    catch exit:Err ->
            %% we can retry some specified number of times, or just
            %% return here? The worker may not be alive. An edge is
            %% that the `worker` was only alive for a reconnect delay
            %% window, and terminated before handling this call. This
            %% is why we call rather than cast.
            {error, Err}
    end.

%% @doc generates a module with constant time access to certain
%% configuration fields (namely configured number of connections,
%% names of workers, name of module) Copied librally from
%% sidejob_config.erl
-spec load_producer() -> ok | {error, Reason::term()}.
load_producer() ->
    Config = load_config(),
    Module = make_module(?GENERATED_MOD),
    Exports = [make_export(Key) || {Key, _} <- Config],
    Functions = [make_function(Key, Value) || {Key, Value} <- Config],
    ExportAttr = make_export_attribute(Exports),
    Abstract = [Module, ExportAttr | Functions],
    Forms = erl_syntax:revert_forms(Abstract),
    {ok, Resource, Bin} = compile:forms(Forms, [verbose, report_errors]),
    code:purge(Resource),
    {module, ?GENERATED_MOD} = code:load_binary(?GENERATED_MOD,
                                                atom_to_list(?GENERATED_MOD) ++ ".erl",
                                                Bin),
    ok.

%% @doc generate the child specs for the producers, call _after_
%% load_producer/0
producer_specs() ->
    ?GENERATED_MOD:specs().

%% @doc again borrowed liberally from basho/sidejob. Picks a worker
%% from the "static" list of workers based on scheduler id of calling
%% process.
get_worker() ->
    get_worker(get_scheduler_id()).

%% @private makes testing easier if we don't need to get a real
%% scheduler ID
get_worker(Scheduler) ->
    Count = ?GENERATED_MOD:count(),
    Worker = Scheduler rem Count,
    worker_name(Worker).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([AMQPURI, Opts]) ->
    %% we want to crash when a connection closes, but we want to wait
    %% a short delay first if it's a very shortlived
    %% process. start_link_channel links to the connection process.
    process_flag(trap_exit, true),
    ReconnectDelay = proplists:get_value(reconnect_delay_millis,
                                         Opts,
                                         ?DEFAULT_RECONN_DELAY_MILLIS),
    {ok, AMQPParams} = rabl_amqp:parse_uri(AMQPURI),
    case start_link_channel(AMQPParams) of
        {ok, Channel} ->
            {ok, Queue} = application:get_env(rabl, cluster_name),
            {ok, connected, #state{channel=Channel,
                                   queue=Queue,
                                   amqp_params=AMQPParams,
                                   reconnect_delay_millis=ReconnectDelay}};
        {error, Error} ->
            lager:error("Connection error ~p for ~p", [Error, AMQPURI]),
            erlang:send_after(ReconnectDelay, self(), connect_timeout),
            {ok, disconnected, #state{reconnect_delay_millis=ReconnectDelay}}
    end.

connected({publish, Msg}, _From,  State) ->
    #state{channel=Channel, queue=Queue} = State,
    Res = rabl_amqp:publish(Channel, Queue, Queue, Msg),
    {reply, Res, connected, State}.

disconnected({publish, _Msg}, _From, State) ->
    {reply, {error, no_connection}, disconnected, State}.

handle_info({'EXIT', Channel, Reason}, _AnyState, State=#state{channel=Channel}) ->
    lager:error("Rabbit Connection Exited with reason ~p", [Reason]),
    #state{start_time_millis=StartTime,
           reconnect_delay_millis=ReconnectDelay} = State,
    Now = rabl_time:monotonic_time(),
    case (Now - StartTime) of
        X when X >= ReconnectDelay ->
            %% remove the (dead)channel from state so terminate
            %% doesn't attempt to close it.
            {stop, connection_error, State#state{channel=undefined}};
        Y ->
            %% wait a bit before crashing
            lager:debug("connection died fast, waiting a bit ~p millis", [ReconnectDelay -Y]),
            erlang:send_after(ReconnectDelay-Y, self(), connect_timeout),
            {next_state, disconnected, State#state{channel=undefined}}
    end;
handle_info(connect_timeout, _AnyState, State) ->
    lager:debug("timeout fired, time to die"),
    {stop, connection_error, State};
handle_info(Other, AnyState, State) ->
    lager:warning("Unexpected Info message ~p~n", [Other]),
    {next_state, AnyState, State}.

handle_event(_Event, AnyState, State) ->
    {next_state, AnyState, State}.

handle_sync_event(_Event, _From, AnyState, State) ->
    {next_state, AnyState, State}.

terminate(_Reason, _StateName, State) ->
    #state{channel=Channel} = State,
    if is_pid(Channel) ->
            rabl_amqp:channel_close(Channel),
            ok;
       true ->
            ok
    end.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private start_link_channel attempts to start a connection, and
%% open an a channel, and link to it, if successful it returns the
%% linked channel pid
-spec start_link_channel(rabl_amqp:amqp_connection_params()) ->
                                {ok, Channel::pid()} |
                                {error, Error::term()}.
start_link_channel(AMQPParams) ->
    case rabl_amqp:connection_start(AMQPParams) of
        {ok, Connection} ->
            link(Connection),
            case rabl_amqp:channel_open(Connection) of
                {ok, Channel} ->
                    link(Channel),
                    {ok, Channel};
                Error ->
                    unlink(Connection),
                    {error, Error}
            end;
        Error ->
            {error, Error}
    end.

%%%===================================================================
%%  Erl_Syntax code Gen :: borrowed from basho/sidejob
%%%===================================================================

make_module(Module) ->
    erl_syntax:attribute(erl_syntax:atom(module),
                         [erl_syntax:atom(Module)]).

make_export(Key) ->
    erl_syntax:arity_qualifier(erl_syntax:atom(Key),
                               erl_syntax:integer(0)).
make_export_attribute(Exports) ->
    erl_syntax:attribute(erl_syntax:atom(export),
                         [erl_syntax:list(Exports)]).

make_function(Key, Value) ->
    Constant = erl_syntax:clause([], none, [erl_syntax:abstract(Value)]),
    erl_syntax:function(erl_syntax:atom(Key), [Constant]).

%% @private creates a proplist from application env variables for the
%% producer, such things as how many, and what to call them
-spec load_config() -> proplists:proplist().
load_config() ->
    %% @TODO(rdb|robust) validate and crash if poorly configured
    ProducerConf = application:get_env(rabl, producers, []),
    ReconnectDelay = application:get_env(rabl, reconnect_delay_millis, ?DEFAULT_RECONN_DELAY_MILLIS),
    {ProducerCount, WorkerNames, ProducerSpecs} = load_config(ProducerConf, ReconnectDelay, {0, [], []}),
    [{count, ProducerCount}, {workers, WorkerNames}, {specs, ProducerSpecs}].

load_config([], _Delay, {Total, Names, Specs}) ->
    {Total, lists:reverse(Names), lists:reverse(Specs)};
load_config([{WorkerCnt, AMQPURI} | Rest], Delay, {Total, Names0, Specs0}) ->
    {Names2, Specs2} = lists:foldl(fun(I, {Names, Specs}) ->
                                           Name = worker_name(I),
                                           Spec = producer_spec(Name, AMQPURI, [{reconnect_delay_millis, Delay}]),
                                           {[Name | Names],
                                            [Spec | Specs]}
                                   end,
                                   {Names0, Specs0},
                                   lists:seq(Total, Total+WorkerCnt-1)),
    load_config(Rest, Delay, {Total+WorkerCnt, Names2, Specs2}).

%% @private generate a child spec for the given `Worker'.
producer_spec(Worker, AMQPURI, Opts) ->
    {Worker,
     {?MODULE, start_link, [Worker, AMQPURI, Opts]},
     permanent,
     5000,
     worker,
     [?MODULE]}.

%% @private generate an atom for a worker name
worker_name(N) when is_integer(N) ->
    list_to_atom(atom_to_list(?GENERATED_MOD)++integer_to_list(N)).

%% @private just to faciliate testing, as erlang module is hard to mock
get_scheduler_id() ->
    erlang:system_info(scheduler_id).

-ifdef(TEST).
%% @doc checks that the module is generated and the property functions
%% return expected values
load_producer_test() ->
    application:set_env(rabl, producers, [{10, "amqp://localhost"}]),
    load_producer(),
    ?assertEqual(10, ?GENERATED_MOD:count()),
    ?assertEqual(10, length(?GENERATED_MOD:workers())),
    ExpectedWorkerList = [
                          rabl_producer_gen0,
                          rabl_producer_gen1,
                          rabl_producer_gen2,
                          rabl_producer_gen3,
                          rabl_producer_gen4,
                          rabl_producer_gen5,
                          rabl_producer_gen6,
                          rabl_producer_gen7,
                          rabl_producer_gen8,
                          rabl_producer_gen9
                         ],
    ?assertEqual(ExpectedWorkerList, ?GENERATED_MOD:workers()).

%% @doc test that a worker per requested producer is added to the
%% worker list
get_worker_test() ->
    application:set_env(rabl, producers, [{10, "amqp://localhost"}]),
    load_producer(),
    Worker = get_worker(32),
    ExpectedWorker = worker_name(2),
    ?assertEqual(ExpectedWorker, Worker).

%% @doc check that the generated module returns the expected child
%% specs for the supervisor to start/manage the producers
specs_test() ->
    application:set_env(rabl, producers, [{3, "amqp://localhost"},
                                          {5, "amqp://otherhost"},
                                          {1, "amqp://totherhost"}]),
    application:set_env(rabl, reconnect_delay_millis, 1000),
    load_producer(),
    Specs = producer_specs(),
    ?assertEqual(9, length(Specs)),
    assertSpecs(Specs, ?GENERATED_MOD:workers()).

%% @private NOTE: has knowledge of the child specs structure, checks
%% that for each worker in Workers there is a matching spec, with the
%% correct start args (id, url, reconnect delay)
assertSpecs([], []) ->
    ?assert(true);
assertSpecs([{Id, {?MODULE, start_link, Args}, permanent, 5000, worker, [?MODULE]} | Specs], [Id | Workers] ) ->
    assertArgs(Id, Args),
    assertSpecs(Specs, Workers);
assertSpecs(Specs, Workers) ->
    ?assertEqual(false, {Specs, Workers}).

assertArgs(Id, Args) ->
    %% NOTE assumes the order of application env {rabl, producers} is
    %% maintained
    {ok, Env} = application:get_env(rabl, producers),
    {ok, ExpectedDelay} =  application:get_env(rabl, reconnect_delay_millis),
    ExpandedUrls = expand_urls(Env),
    Lid = atom_to_list(Id),
    Lmod = atom_to_list(?GENERATED_MOD),
    Lcnt = lists:sublist(Lid, length(Lmod)+1, length(Lid) - length(Lmod)),
    Cnt = list_to_integer(Lcnt),
    ExpectedUrl = lists:nth(Cnt+1, ExpandedUrls),

    ?assertEqual(3, length(Args)),
    ?assertEqual(Id, hd(Args)),
    ?assertEqual(ExpectedUrl, lists:nth(2, Args)),
    ?assertMatch([{reconnect_delay_millis, ExpectedDelay}],
                 lists:nth(3, Args)).

expand_urls(Env) ->
    lists:foldl(fun({N, Url}, Acc) ->
                        Acc ++ lists:duplicate(N, Url)
                end,
                [],
                Env).

%% @doc test that a started producer does not crash at once if it
%% cannot connect to rabbitmq on start up
no_connected_at_start_delay_test() ->
    Delay = 1000,
    application:set_env(rabl, producers, [{1, "amqp://localhost"}]),
    application:set_env(rabl, reconnect_delay_millis, Delay),
    load_producer(),
    Start = rabl_time:monotonic_time(),
    {ok, Pid} = start_link(test1, "amqp://localhost", [{reconnect_delay_millis, Delay}]),
    unlink(Pid),
    MonRef = monitor(process, Pid),
    receive {'DOWN', MonRef, process, Pid, connection_error} ->
            Now = rabl_time:monotonic_time(),
            ?assert(Now-Start >= Delay)
    after 1500 ->
            ?assert(false)
    end.

%% @doc test that a started producer _does_ crash at once if it has
%% been alive longer than a configured reconnect delay
crash_no_delay_test() ->
    Delay = 500,
    application:set_env(rabl, producers, [{1, "amqp://localhost"}]),
    application:set_env(rabl, reconnect_delay_millis, Delay),
    application:set_env(rabl, cluster_name, <<"test_cluster">>),

    load_producer(),

    meck:new(rabl_amqp, [passthrough]),
    Pid = self(),
    meck:expect(rabl_amqp, connection_start, fun(_) ->
                                                     {ok, rabl_mock:mock_rabl_con(Pid)}
                                             end),
    meck:expect(rabl_amqp, channel_open, fun(_) ->
                                                 {ok, rabl_mock:mock_rabl_chan(Pid)}
                                         end),

    {ok, Producer} = start_link(test1, "amqp://localhost", [{reconnect_delay_millis, Delay}]),
    unlink(Producer),
    MonRef = monitor(process, Producer),

    Channel = rabl_mock:receive_channel(Pid),
    %% just kind of be alive for longer than the reconnect delay
    timer:sleep(1000),

    Start = rabl_time:monotonic_time(),

    exit(Channel, kill),

    receive {'DOWN', MonRef, process, Producer, connection_error} ->
            Now = rabl_time:monotonic_time(),
            %% using time like this is inherently dodgy, it would be
            %% better to get a history trace from the FSM and show
            %% that it never entered the `disconnected' state, but
            %% went from `connected' to `terminate'. I should figure
            %% out how to do that.
            ?assert(Now-Start < (Delay/2))
    after 1500 ->
            ?assert(false)
    end,
    meck:unload(rabl_amqp).

%% @doc test that a started producer doesn't crash at once if it has
%% been connected shorter than the reconnect delay
crash_delay_test() ->
    Delay = 1000,
    application:set_env(rabl, producers, [{1, "amqp://localhost"}]),
    application:set_env(rabl, reconnect_delay_millis, Delay),
    application:set_env(rabl, cluster_name, <<"test_cluster">>),

    load_producer(),

    meck:new(rabl_amqp, [passthrough]),
    Pid = self(),
    meck:expect(rabl_amqp, connection_start, fun(_) ->
                                                     {ok, rabl_mock:mock_rabl_con(Pid)}
                                             end),
    meck:expect(rabl_amqp, channel_open, fun(_) ->
                                                 {ok, rabl_mock:mock_rabl_chan(Pid)}
                                         end),

    {ok, Producer} = start_link(test1, "amqp://localhost", [{reconnect_delay_millis, Delay}]),
    unlink(Producer),
    MonRef = monitor(process, Producer),
    Start = rabl_time:monotonic_time(),

    Channel = rabl_mock:receive_channel(Pid),
    %% be alive for less than the delay
    timer:sleep(200),

    exit(Channel, kill),

    receive {'DOWN', MonRef, process, Producer, connection_error} ->
            Now = rabl_time:monotonic_time(),
            %% using time like this is inherently dodgy, it would be
            %% better to get a history trace from the FSM and show
            %% that it never entered the `disconnected' state, but
            %% went from `connected' to `terminate'. I should figure
            %% out how to do that.
            ?assert(Now-Start >= Delay)
    after 1500 ->
            ?assert(false)
    end,
    meck:validate(rabl_amqp),
    meck:unload(rabl_amqp).

%% @doc checks that publish when connected ends with a call to
%% rabl_amqp publish on the producers channel
publish_connected_test() ->
    ClusterName = <<"test_cluster">>,
    Delay = 1000,
    application:set_env(rabl, producers, [{1, "amqp://localhost"}]),
    application:set_env(rabl, reconnect_delay_millis, Delay),
    application:set_env(rabl, cluster_name, ClusterName),

    load_producer(),

    meck:new(rabl_amqp, [passthrough]),
    Pid = self(),
    meck:expect(rabl_amqp, connection_start, fun(_) ->
                                                     {ok, rabl_mock:mock_rabl_con(Pid)}
                                             end),
    meck:expect(rabl_amqp, channel_open, fun(_) ->
                                                 {ok, rabl_mock:mock_rabl_chan(Pid)}
                                         end),

    {ok, Producer} = start_link(hd(?GENERATED_MOD:workers()), "amqp://localhost", [{reconnect_delay_millis, Delay}]),
    unlink(Producer),

    Channel = rabl_mock:receive_channel(Pid),
    %% match the expect on real arguments
    meck:expect(rabl_amqp, publish, [Channel, ClusterName, ClusterName, <<"test">>],
                meck:val(ok)),

    PubRes = publish(<<"test">>),
    ?assertEqual(ok, PubRes),
    meck:validate(rabl_amqp),
    meck:unload(rabl_amqp),
    %% @TODO do it better, please.
    exit(Producer, kill),
    ok.

%% @doc checks that publish returns an error when the producer is not
%% connected.
publish_disconnected_test() ->
ClusterName = <<"test_cluster">>,
    Delay = 1000,
    application:set_env(rabl, producers, [{1, "amqp://localhost"}]),
    application:set_env(rabl, reconnect_delay_millis, Delay),
    application:set_env(rabl, cluster_name, ClusterName),

    load_producer(),

    meck:new(rabl_amqp, [passthrough]),
    Pid = self(),
    meck:expect(rabl_amqp, connection_start, fun(_) ->
                                                     {ok, rabl_mock:mock_rabl_con(Pid)}
                                             end),
    meck:expect(rabl_amqp, channel_open, fun(_) ->
                                                 {ok, rabl_mock:mock_rabl_chan(Pid)}
                                         end),

    {ok, Producer} = start_link(hd(?GENERATED_MOD:workers()), "amqp://localhost", [{reconnect_delay_millis, Delay}]),
    unlink(Producer),
    MonRef = monitor(process, Producer),

    Channel = rabl_mock:receive_channel(Pid),
    exit(Channel, kill),
    %% I hate this sleep being here, but how else do we ensure that
    %% the `Down' message is received before the publish message?
    %% Equally, highlights the race, eh?
    timer:sleep(10),

    PubRes = rabl_producer_fsm:publish(<<"test">>),
    ?assertEqual({error, no_connection}, PubRes),

    receive {'DOWN', MonRef, process, Producer, connection_error} ->
            ?assert(true)
    after 1500 ->
            ?assert(false)
    end,

    meck:validate(rabl_amqp),

    meck:unload(rabl_amqp),

    ok.

-endif.
