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
-export([start_link/2, publish/1, status/1, workers/0,
         load_producer/0, get_worker/0, producer_specs/0]).

%% gen_fsm callbacks
-export([init/1, connected/3,
         disconnected/3,
         handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

-define(GENERATED_MOD, rabl_producer_gen).
-define(DEFAULT_RECONN_DELAY_MILLIS, 50).
-define(DEFAULT_MAX_RECONN_DELAY_MILLIS, 10000).

-record(state, {
          channel,
          connection, %% needed for clean shutdown
          queue,
          amqp_params,
          connection_start_time,
          reconnect_delay_millis=?DEFAULT_RECONN_DELAY_MILLIS,
          connection_attempt_counter = 0
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
                 AMQPURI::string()) ->
                        {ok, Pid::pid()}
                            | ignore
                            | {error, Error::term()}.
start_link(Name, AMQPURI) ->
    gen_fsm:start_link({local, Name}, ?MODULE, [AMQPURI], []).

%%--------------------------------------------------------------------
%% @doc
%% Publish a replication message
%% @end
%%--------------------------------------------------------------------
-spec publish(Msg::binary()) -> ok.
publish(Msg) ->
    case get_worker() of
        {error, no_workers} ->
            {error, no_workers};
        {ok, Worker} ->
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
            end
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
-spec workers() -> [atom()].
workers() ->
    ?GENERATED_MOD:workers().


-spec status(atom()) -> {atom(), Status::term()}.
status(Worker) ->
    Status = (catch gen_fsm:sync_send_all_state_event(Worker, status, 5000)),
    {Worker, Status}.

%% @doc again borrowed liberally from basho/sidejob. Picks a worker
%% from the "static" list of workers based on scheduler id of calling
%% process.
-spec get_worker() -> {ok, atom()} | {error, no_workers}.
get_worker() ->
    get_worker(get_scheduler_id()).

%% @private makes testing easier if we don't need to get a real
%% scheduler ID
-spec get_worker(non_neg_integer()) ->
                        {ok, atom()} |
                        {error, no_workers}.
get_worker(Scheduler) ->
    case ?GENERATED_MOD:count() of
        Count when Count > 0 ->
            Worker = Scheduler rem Count,
            {ok, worker_name(Worker)};
        _ ->
            {error, no_workers}
    end.

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================
init([AMQPURI]) ->
    %% we don't want to crash if the connection does
    process_flag(trap_exit, true),
    ReconnectDelay = default_reconnect_delay(),
    {ok, Queue} = application:get_env(rabl, cluster_name),
    {ok, AMQPParams} = rabl_amqp:parse_uri(AMQPURI),

    State = #state{amqp_params=AMQPParams,
                   queue=Queue,
                   reconnect_delay_millis=ReconnectDelay},
    case connect(State) of
        {true, State2} ->
            {ok, connected, State2};
        {false, State2} ->
            {ok, disconnected, State2}
    end.

%% @private called by publish when the fsm in the connected state
connected({publish, Msg}, _From,  State) ->
    #state{channel=Channel, queue=Queue} = State,
    Res = rabl_amqp:publish(Channel, Queue, Queue, Msg),
    {reply, Res, connected, State}.

%% @private called by publish when the fsm is disconnected
disconnected({publish, Msg}, _From, State) ->
    case connect(State) of
        {true, State2} ->
            #state{queue=Queue, channel=Channel} = State2,
            Res = rabl_amqp:publish(Channel, Queue, Queue, Msg),
            {reply, Res, connected, State};
        {false, State2} ->
            {reply, {error, no_connection}, disconnected, State2}
    end.

%% info callbacks
%% Called when the channel exits
handle_info({'EXIT', Channel, Reason}, _AnyState, State=#state{channel=Channel}) ->
    lager:error("Rabbit Connection Exited with reason ~p", [Reason]),
    case connect(State) of
        {true, State2} ->
            {next_state, connected, State2};
        {false, State2} ->
            {next_state, disconnected, State2}
    end;
%% called by a timer set in this module
handle_info(connect_timeout, disconnected, State) ->
    case connect(State) of
        {true, State2} ->
            {next_state, connected, State2};
        {false, State2} ->
            {next_state, disconnected, State2}
    end;
handle_info(connect_timeout, connected, State) ->
    %% assume a timer message arrived after a publish call forced
    %% reconnect.
    {next_state, connected, State};
handle_info(Other, AnyState, State) ->
    case rabl_amqp:receive_return(Other) of
        {rabbit_return, Reason, Msg} ->
            rabl_stat:return(),
            log_return(Msg, Reason);
        false ->
            lager:debug("Unexpected Info message ~p~n", [Other])
    end,
    {next_state, AnyState, State}.

handle_event(_Event, AnyState, State) ->
    {next_state, AnyState, State}.

handle_sync_event(status, _From, AnyState, State) ->
    Status = handle_status(AnyState, State),
    {reply, Status, AnyState, State};
handle_sync_event(_Event, _From, AnyState, State) ->
    {next_state, AnyState, State}.

terminate(_Reason, _StateName, State) ->
    #state{channel=Channel, connection=Connection} = State,
    case is_pid(Channel) andalso is_process_alive(Channel)
    andalso is_pid(Connection) andalso is_process_alive(Connection) of
        true ->
            disconnect(Connection, Channel);
        false ->
            ok
    end.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
disconnect(Connection, Channel) ->
    unlink(Connection),
    unlink(Channel),
    rabl_amqp:channel_close(Channel),
    rabl_amqp:connection_close(Connection),
    ok.

-spec connect(#state{}) -> {boolean(), #state{}}.
connect(State) ->
    #state{connection_attempt_counter=CAC,
           reconnect_delay_millis=Delay0,
           amqp_params=AMQPParams,
           queue=Queue} = State,
    {ok, MaxRetries} = application:get_env(rabl, max_connection_retries),
    case CAC >= MaxRetries of
        true ->
            %% stop trying, the next publish attempt will force a
            %% reconnect attempt. We can start all over the
            %% connection_attempt_counter and backoff delay variables
            ResetDelay = default_reconnect_delay(),
            {false, State#state{channel=undefined,
                                connection=undefined,
                                connection_start_time=undefined,
                                connection_attempt_counter=0,
                                reconnect_delay_millis=ResetDelay}};
        false ->
            %% try and reconnect, schedule a retry on fail
            case start_link_channel(AMQPParams, Queue) of
                {ok, Channel, Connection} ->
                    ResetDelay = default_reconnect_delay(),
                    NewState = State#state{channel=Channel,
                                           connection=Connection,
                                           connection_start_time=os:timestamp(),
                                           connection_attempt_counter=0,
                                           reconnect_delay_millis=ResetDelay},
                    {true, NewState};
                {error, Error} ->
                    lager:error("Connection error ~p for ~p", [Error, AMQPParams]),
                    erlang:send_after(Delay0, self(), connect_timeout),
                    Delay = backoff(Delay0),
                    {false, State#state{reconnect_delay_millis=Delay,
                                        connection_attempt_counter=CAC+1}}
            end
    end.

%% @private basic backoff calculation, with a maximum delay set by app
%% env var.
backoff(N) ->
    backoff(N, application:get_env(rabl, max_reconnect_delay_millis,
                                   ?DEFAULT_MAX_RECONN_DELAY_MILLIS)).

%% @private exponential backoff with a maximum
backoff(N, Max) ->
    min(N*2, Max).

%% @private the reconnect delay reset to the default/app specified
default_reconnect_delay() ->
    application:get_env(rabl, reconnect_delay_millis,
                        ?DEFAULT_RECONN_DELAY_MILLIS).

%% @private log a return message
log_return(Msg, Reason) ->
    case rabl_codec:decode(Msg) of
        {Time, BK, _Obj} ->
            lager:warning("Replication message for ~p at ~p returned with reason ~p", [BK, Time, Reason]);
        {error, DecError} ->
            lager:error("Replication message returned with reason ~p, unable to decode msg with error ~p",
                        [Reason, DecError])
    end.

%% @private start_link_channel attempts to start a connection, and
%% open an a channel, and link to it, if successful it returns the
%% linked channel pid
-spec start_link_channel(rabl_amqp:amqp_connection_params(), Queue::binary()) ->
                                {ok, Channel::pid()} |
                                {error, Error::term()}.
start_link_channel(AMQPParams, Queue) ->
    case rabl_amqp:connection_start(AMQPParams) of
        {ok, Connection} ->
            link(Connection),
            case rabl_amqp:channel_open(Connection) of
                {ok, Channel} ->
                    link(Channel),
                    rabl_amqp:register_return_handler(Channel, self()),
                    rabl_util:try_ensure_exchange(Channel, Queue),
                    {ok, Channel, Connection};
                Error ->
                    unlink(Connection),
                    rabl_amqp:connection_close(Connection),
                    {error, Error}
            end;
        Error ->
            {error, Error}
    end.

%% @private generate a status report for this worker
-spec handle_status(StateName::atom(), State::#state{}) -> proplists:proplist().
handle_status(StateName, State) ->
    #state{connection_start_time=StartTime,
           amqp_params=Params} = State,
    case StateName of
        connected ->
            ConnTime = timer:now_diff(os:timestamp(), StartTime),
            [{connection_time, ConnTime},
             {connection_params, Params},
             {status, connected}];
        disconnected ->
            [{status, disconnected},
             {connection_params, Params}]
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
    {ProducerCount, WorkerNames, ProducerSpecs} = load_config(ProducerConf, {0, [], []}),
    [{count, ProducerCount}, {workers, WorkerNames}, {specs, ProducerSpecs}].

load_config([], {Total, Names, Specs}) ->
    {Total, lists:reverse(Names), lists:reverse(Specs)};
load_config([{WorkerCnt, AMQPURI} | Rest], {Total, Names0, Specs0}) ->
    {Names2, Specs2} = lists:foldl(fun(I, {Names, Specs}) ->
                                           Name = worker_name(I),
                                           Spec = producer_spec(Name, AMQPURI),
                                           {[Name | Names],
                                            [Spec | Specs]}
                                   end,
                                   {Names0, Specs0},
                                   lists:seq(Total, Total+WorkerCnt-1)),
    load_config(Rest, {Total+WorkerCnt, Names2, Specs2}).

%% @private generate a child spec for the given `Worker'.
producer_spec(Worker, AMQPURI) ->
    {Worker,
     {?MODULE, start_link, [Worker, AMQPURI]},
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
    {ok, Worker} = get_worker(32),
    ExpectedWorker = worker_name(2),
    ?assertEqual(ExpectedWorker, Worker).

%% @doc tests when no configured workers are added
get_no_workers_test() ->
    %% NOTE: no config
    application:set_env(rabl, producers, []),
    load_producer(),
    ?assertMatch({error, no_workers}, get_worker(32)).

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
    ExpandedUrls = expand_urls(Env),
    Lid = atom_to_list(Id),
    Lmod = atom_to_list(?GENERATED_MOD),
    Lcnt = lists:sublist(Lid, length(Lmod)+1, length(Lid) - length(Lmod)),
    Cnt = list_to_integer(Lcnt),
    ExpectedUrl = lists:nth(Cnt+1, ExpandedUrls),

    ?assertEqual(2, length(Args)),
    ?assertEqual(Id, hd(Args)),
    ?assertEqual(ExpectedUrl, lists:nth(2, Args)).

expand_urls(Env) ->
    lists:foldl(fun({N, Url}, Acc) ->
                        Acc ++ lists:duplicate(N, Url)
                end,
                [],
                Env).

%% @doc test that a started producer does not crash if it cannot
%% connect, and that it eventually stops trying
no_connected_at_start_test() ->
    Delay = 1,
    Retries = 10,
    application:set_env(rabl, producers, [{1, "amqp://localhost"}]),
    application:set_env(rabl, reconnect_delay_millis, Delay),
    application:set_env(rabl, cluster_name, <<"test">>),
    application:set_env(rabl, max_connection_retries, Retries),
    load_producer(),

    meck:new(rabl_amqp, [passthrough]),

    meck:expect(rabl_amqp, connection_start, [{1, meck:val({error, nope})}]),

    {ok, Producer} = start_link(test1, "amqp://localhost"),
    unlink(Producer),
    MonRef = monitor(process, Producer),

    meck:wait(10, rabl_amqp, connection_start,['_'], 2000),

    receive {'DOWN', MonRef, process, Producer, _Error} ->
            ?assert(false)
    after 1500 ->
            ?assert(true)
    end,

    ?assertMatch({disconnected, #state{connection_attempt_counter=0}}, sys:get_state(Producer)),
    meck:validate(rabl_amqp),
    exit(Producer, kill),
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

    {ok, Producer} = start_link(hd(?GENERATED_MOD:workers()), "amqp://localhost"),
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

%% @doc checks that producer reconnects on publish if disconnected
publish_disconnected_test() ->
    ClusterName = <<"test_cluster">>,
    Delay = 100,
    Retries = 1, %% i.e. don't retry
    application:set_env(rabl, producers, [{1, "amqp://localhost"}]),
    application:set_env(rabl, reconnect_delay_millis, Delay),
    application:set_env(rabl, cluster_name, ClusterName),
    application:set_env(rabl, max_connection_retries, Retries),
    lager:start(),
    load_producer(),

    meck:new(rabl_amqp, [passthrough]),
    Pid = self(),
    meck:expect(rabl_amqp, connection_start, ['_'], meck:seq([meck:exec(fun(_) -> {ok, rabl_mock:mock_rabl_con(Pid)} end),
                                                              meck:val({error, nope}),
                                                              meck:exec(fun(_) -> {ok, rabl_mock:mock_rabl_con(Pid)} end)
                                                             ])),
    meck:expect(rabl_amqp, channel_open, fun(_) ->
                                                 {ok, rabl_mock:mock_rabl_chan(Pid)}
                                         end),

    {ok, Producer} = start_link(hd(?GENERATED_MOD:workers()), "amqp://localhost"),
    unlink(Producer),

    Channel = rabl_mock:receive_channel(Pid),
    exit(Channel, kill),
    %% I hate this sleep being here, but how else do we ensure that
    %% the `Down' message is received before the publish message?
    %% Equally, highlights the race, eh?
    timer:sleep(1000),

    ?assertMatch({disconnected, #state{connection_attempt_counter=0}}, sys:get_state(Producer)),
    PubRes = rabl_producer_fsm:publish(<<"test">>),
    ?assertEqual(ok, PubRes),
    ?assertMatch({connected, _}, sys:get_state(Producer)),

    exit(Producer, kill),

    meck:validate(rabl_amqp),
    meck:unload(rabl_amqp),

    ok.

-endif.
