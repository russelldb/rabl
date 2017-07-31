%%%-------------------------------------------------------------------
%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%%  very simple fsm consumer of rabl messages. Most of the work is
%%%  managing the connection.
%%% @end
%%% @TODO(rdb|refactor) clean up/refactor connect fun
%%% @TODO(rdb|refactor) maybe move tests to own module
%%% Created : 12 Jun 2017 by Russell Brown <russell@wombat.me>
%%%-------------------------------------------------------------------
-module(rabl_consumer_fsm).

-behaviour(gen_fsm).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/2, status/1]).

%% gen_fsm callbacks
-export([init/1,
         disconnected/2,
         unsubscribed/2,
         consuming/2,
         handle_info/3,
         terminate/3,
         handle_event/3,
         handle_sync_event/4,
         code_change/4]).

-define(SERVER, ?MODULE).
-define(RABL_PUT_OPTS, [asis, disable_hooks]).

-type consumer_state() :: disconnected | unsubscribed | consuming.

-record(state, {
          amqp_params,
          channel,
          channel_monitor,
          connection,
          connection_start_time,
          connection_attempt_counter = 0,
          connection_monitor,
          reconnect_delay_millis,
          riak_client,
          subscription_tag,
          per_q_stat_name
         }).

-define(DEFAULT_RECONN_DELAY_MILLIS, 50).
-define(DEFAULT_MAX_RECONN_DELAY_MILLIS, 10000).
-define(DEFAULT_PREFETCH, 10).

%%%===================================================================
%%% API
%%%===================================================================
-spec start_link(Name::atom(), AMQPUri::string()) -> {ok, pid()} | ignore | {error, Error::any()}.
start_link(Name, AMQPURI) ->
    gen_fsm:start_link({local, Name}, ?MODULE, [AMQPURI], []).

-spec status(atom()) -> {atom(), term()}.
status(Worker) ->
    Status = (catch gen_fsm:sync_send_all_state_event(Worker, status, 5000)),
    {Worker, Status}.

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%%--------------------------------------------------------------------
-spec init(Args::list()) -> {ok, StateName::atom(), #state{}} |
                    {ok, consumer_state(), #state{}, timeout()} |
                    ignore |
                    {stop, StopReason::atom()}.
init([AMQPURI]) ->
    {ok, AMQPParams} = rabl_amqp:parse_uri(AMQPURI),
    {ok, RiakClient} = rabl_riak:client_new(),
    PerQStatName = rabl_stat:params_to_stat_name(AMQPParams),
    ReconnectDelay = application:get_env(rabl, reconnect_delay_millis, ?DEFAULT_RECONN_DELAY_MILLIS),
    {ok, disconnected, #state{amqp_params=AMQPParams,
                              riak_client=RiakClient,
                              per_q_stat_name=PerQStatName,
                              reconnect_delay_millis=ReconnectDelay}, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc disconnected/2 called via the fsm timeout mechanism to attempt to reconnect
%% to rabbitmq. If it fails enough times, exit, and let the supervisor
%% restart this consumer.
%% @end
%%--------------------------------------------------------------------
-spec disconnected(timeout, #state{}) ->
                          {next_state, consumer_state(), #state{}} |
                          {next_state, consumer_state(), #state{}, timeout()} |
                          {stop, Reason::atom(), #state{}}.
disconnected(timeout, State) ->
    lager:info("timeout in state disconnected"),
    connect(State);
disconnected(connect_timeout, State) ->
    lager:info("connect_timeout in state disconnected"),
    connect(State).

%%--------------------------------------------------------------------
%% @private
%% @doc connected/2 never called? Then why an fsm???
%% @end
%%--------------------------------------------------------------------
-spec unsubscribed(Event::any(), #state{}) ->
                       {next_state, consumer_state(), #state{}} |
                       {next_state, consumer_state(), #state{}, timeout()} |
                       {stop, Reason::atom(), #state{}}.
unsubscribed(connect_timeout, State) ->
    lager:info("connect_timeout in state unsubscribed"),
    subscribe(State).

%%--------------------------------------------------------------------
%% @private
%% @doc consuming/2 never called? Then why an fsm???
%% @end
%%--------------------------------------------------------------------
-spec consuming(Event::any(), #state{}) ->
                       {next_state, consumer_state(), #state{}} |
                       {next_state, consumer_state(), #state{}, timeout()} |
                       {stop, Reason::atom(), #state{}}.
consuming(_Event, State) ->
    {next_state, consuming, State}.


-spec handle_event(Event::term(), consumer_state(), #state{}) ->
                       {next_state, consumer_state(), #state{}} |
                       {next_state, consumer_state(), #state{}, timeout()} |
                       {stop, Reason::atom(), #state{}}.
handle_event(Event, StateName, State) ->
    lager:info("Unexpected all state event ~p when in state ~p~n", [Event, StateName]),
    {next_state, StateName, State}.

-spec handle_sync_event(Event::term(), From::{pid(), Tag::any()},
                        consumer_state(), #state{}) ->
                                {next_state, consumer_state(), #state{}}.
handle_sync_event(status, _From, StateName, State) ->
    Status = handle_status(StateName, State),
    {reply, Status, StateName, State};
handle_sync_event(Event, From, StateName, State) ->
    lager:info("Unexpected sync all state event ~p from ~p when in state ~p~n", [Event, From, StateName]),
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private

%% @doc This is the sole api to the external world. This consumer fsm
%% is subscribed to rabbitmq via its connection/channel. When a
%% message is delivered, handle_info is called. When the channel or
%% connection is closed, handle_info is called.  @end
%% --------------------------------------------------------------------

-spec handle_info(Info::any(), consumer_state(), #state{})->
                         {next_state, consumer_state(), #state{}} |
                         {next_state, consumer_state(), #state{}, timeout()} |
                         {stop, Reason::atom(), #state{}}.
handle_info({'DOWN', ChanMonRef, process, Channel, Reason},
            _AnyState,
            State=#state{channel_monitor=ChanMonRef, channel=Channel}) ->
    lager:error("Channel Down with reason ~p~n", [Reason]),
    %% Channel is closed, try and open another (but probably
    %% connection is closed too and that down message is yet to be
    %% delivered.)
    #state{connection=Connection, connection_monitor=ConMonRef} = State,
    case rabl_amqp:channel_open(Connection) of
        {ok, NewChannel} ->
            NewChanMonRef = erlang:monitor(process, NewChannel),
            State2 = State#state{channel_monitor=NewChanMonRef, channel=NewChannel},
            {next_state, connected, State2};
        _Error ->
            %% Assume that the connection is broken
            erlang:demonitor(ConMonRef, [flush]),
            rabl_amqp:connection_close(Connection),
            connect(State)
    end;
handle_info({'DOWN', ConnMonRef, process, Connection, Reason},
            _AnyState,
            State=#state{connection_monitor=ConnMonRef, connection=Connection}) ->
    lager:error("Connection Down with reason ~p~n", [Reason]),
    connect(State);
handle_info(connect_timeout, StateName, State) ->
    ?MODULE:StateName(connect_timeout, State);
handle_info(Info, AnyState, State) ->
    %% TODO implement throttle here?
    Time = os:timestamp(),
    case rabl_amqp:receive_msg(Info) of
        {rabbit_msg, ok} -> {next_state, AnyState, State};
        {rabbit_msg, cancel} ->
            lager:info("Cancel message received, stopping~n"),
            {stop, subscription_cancelled, State};
        {rabbit_msg, {msg, Message, Tag}} ->
            #state{riak_client=Client,
                   channel=Channel,
                   per_q_stat_name=StatName} = State,
            {PublishTime, {B, K}, BinObj} = binary_to_term(Message),
            rabl_stat:consume(StatName, PublishTime, Time),

            Obj = rabl_riak:object_from_binary(B, K, BinObj),
            lager:debug("rabl putting ~p ~p~n", [B, K]),
            StartPut = os:timestamp(),
            case rabl_riak:client_put(Client, Obj, ?RABL_PUT_OPTS) of
                ok ->
                    EndPut = os:timestamp(),
                    %% @TODO something about channel/ack failures
                    ok = rabl_amqp:ack_msg(Channel, Tag),
                    rabl_stat:riak_put(success, StartPut, EndPut);
                Error ->
                    %% @TODO (consider client timeouts | overload etc
                    %% here. eg SLEEP if overload?)
                    ok = rabl_amqp:nack_msg(Channel, Tag),
                    EndPut = os:timestamp(),
                    rabl_stat:riak_put(fail, StartPut, EndPut),
                    lager:error("Failed to consume replicated object ~p ~p with Error ~p", [B, K, Error])
            end,
            {next_state, AnyState, State};
        OtherInfo ->
            %% things that are 'DOWN' messages not caught above? What
            %% exactly (stale down from connection after channel
            %% down?) Maybe log?
            lager:info("received unexpected info message ~p~n", [OtherInfo]),
            {next_state, AnyState, State}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, disconnected, _State) ->
    ok;
terminate(_Reason, _, State) ->
    disconnect(State).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec connect(#state{}) -> {next_state, connected, #state{}} |
                           {next_state, disconnected, #state{}, timeout()} |
                           {next_state, unsubscribed, #state{}, timeout()} |
                           {stop, max_connection_retry_limit_reached, #state{}}.
connect(State) ->
    #state{amqp_params=AMQPParams,
           reconnect_delay_millis=Delay,
           connection_attempt_counter=CAC} = State,
    {ok, MaxRetries} = application:get_env(rabl, max_connection_retries),
    PrefetchCount = application:get_env(rabl, prefetch_count, ?DEFAULT_PREFETCH),
    Delay2 = backoff(Delay),

    lager:info("connection ~p of ~p with delay ~p", [CAC, MaxRetries, Delay]),

    case CAC >= MaxRetries of
        true ->
            lager:info("Giving up and dying"),
            {stop, max_connection_retry_limit_reached, State};
        false ->
            case rabl_amqp:connection_start(AMQPParams) of
                {ok, Connection} ->
                    ConnMonRef = erlang:monitor(process, Connection),
                    case rabl_amqp:channel_open(Connection) of
                        {ok, Channel} ->
                            ChanMonRef = erlang:monitor(process, Channel),
                            ok = rabl_amqp:set_prefetch_count(Channel, PrefetchCount),
                            subscribe(State#state{channel=Channel,
                                                  channel_monitor=ChanMonRef,
                                                  connection_monitor=ConnMonRef,
                                                  connection=Connection,
                                                  connection_start_time=os:timestamp(),
                                                  reconnect_delay_millis=50,
                                                  connection_attempt_counter=0});
                        ChannelOpenError ->
                            %% The type of Error is unspecified in rabbit docs
                            %% (and code!) good jawb!
                            lager:error("Error ~p opening a channel on connection with params ~p~n", [ChannelOpenError, AMQPParams]),
                            erlang:demonitor(ConnMonRef, [flush]),
                            ok = rabl_amqp:connection_close(Connection),
                            erlang:send_after(Delay, self(), connect_timeout),
                            {next_state, disconnected, State#state{reconnect_delay_millis=Delay2,
                                                                   connection_attempt_counter=CAC+1}}
                    end;
                {error, ConnectionOpenError} ->
                    lager:error("Error ~p connecting with params ~p~n", [ConnectionOpenError, AMQPParams]),
                    erlang:send_after(Delay, self(), connect_timeout),
                    {next_state, disconnected, State#state{reconnect_delay_millis=Delay2,
                                                           connection_attempt_counter=CAC+1}}
            end
    end.

%% @private eg for when terminate is called.
-spec disconnect(#state{}) -> ok.
disconnect(State) ->
    #state{connection=Connection, connection_monitor=ConnMonRef,
           channel=Channel, channel_monitor=ChanMonRef} = State,
    erlang:demonitor(ConnMonRef, [flush]),
    erlang:demonitor(ChanMonRef, [flush]),
    rabl_amqp:channel_close(Channel),
    rabl_amqp:connection_close(Connection),
    ok.

%% @private subscribe to the configured queue
-spec subscribe(#state{}) ->
                       {next_state, consumer_state(), #state{}, timeout()}
                           | {next_state, consumer_state(), #state{}}
                           | {stop, max_subscribe_retry_limit_reached, #state{}}.
subscribe(State) ->
    #state{channel=Channel,
           reconnect_delay_millis=Delay,
           connection_attempt_counter=CAC} = State,
    {ok, SinkQueue} = application:get_env(rabl, sink_queue),
    {ok, MaxRetries} = application:get_env(rabl, max_connection_retries),

    rabl_util:try_ensure_exchange(Channel, SinkQueue),

    case CAC >= MaxRetries of
        true ->
            {stop, max_subscribe_retry_limit_reached, State};
        false ->
            try rabl_amqp:subscribe(Channel, SinkQueue, self()) of
                {ok, Tag} ->
                    State2 = State#state{subscription_tag=Tag},
                    lager:info("consumer ready"),
                    {next_state, consuming, State2};
                Error ->
                    lager:error("Error ~p subscribing to ~p ~n", [Error, SinkQueue]),
                    Delay2 = backoff(Delay),
                    erlang:send_after(Delay, self(), connect_timeout),
                    {next_state, unsubscribed, State#state{reconnect_delay_millis=Delay2,
                                                           connection_attempt_counter=CAC+1}}
            catch exit:Ex when is_tuple(Ex) andalso element(1, Ex) == noproc ->
                    %% connection | channel busted, destroy and start
                    %% over
                    disconnect(State),
                    erlang:send_after(0, self(), connect_timeout),
                    {next_state, disconnected, State}
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

%% @private generate a status report for this worker
-spec handle_status(StateName::atom(), State::#state{}) -> proplists:proplist().
handle_status(StateName, State) ->
    #state{connection_start_time=StartTime,
           amqp_params=Params} = State,
    case StateName of
        consuming ->
            ConnTime = timer:now_diff(os:timestamp(), StartTime),
            [{connection_time, ConnTime},
             {connection_params, Params},
             {status, consuming}];
        _ ->
            [{status, disconnected},
             {connection_params, Params}]
    end.

%% TESTS
-ifdef(TEST).

-define(AMQPURI, "amqp://localhost").

fail_on_max_conn_attempts_test_() ->
    {timeout, 10, fun fail_on_max_conn_attempts/0}.

fail_on_max_channel_open_errors_test_() ->
    {timeout, 10, fun fail_on_max_channel_open_errors/0}.

reopen_closed_connections_test_() ->
    {timeout, 10, fun reopen_closed_connections/0}.

reopen_closed_channels_test_() ->
    {timeout, 10, fun reopen_closed_channels/0}.

reopen_closed_channels_and_connections_test_() ->
    {timeout, 10, fun reopen_closed_channels_and_connections/0}.

fail_on_max_subscribe_errors_test_() ->
    {timeout, 10, fun fail_on_max_subscribe_errors/0}.

subscribe_noproc_reconnects_test_() ->
    {timeout, 10, fun subscribe_noproc_reconnects/0}.

handles_rabbit_messages_test_() ->
    {timeout, 10, fun handles_rabbit_messages/0}.

fail_on_max_conn_attempts() ->
    rabl_env_setup(),
    meck:new(rabl_amqp, [passthrough]),
    meck:expect(rabl_amqp, connection_start, ['_'], meck:val({error, econnrefused})),
    mock_riak(),

    {ok, Consumer} = start_link(test, ?AMQPURI),
    unlink(Consumer),
    Mon = monitor(process, Consumer),
    ?assertMatch(ok, meck:wait(5, rabl_amqp, connection_start, ['_'], 5000)),

    receive
        {'DOWN', Mon, process, Consumer, max_connection_retry_limit_reached} ->
            ok
    end,

    ?assertMatch(undefined, process_info(Consumer)),
    meck:unload(rabl_amqp),
    unmock_riak().

fail_on_max_channel_open_errors() ->
    rabl_env_setup(),
    mock_riak(),
    Pid = self(),
    meck:new(rabl_amqp, [passthrough]),
    meck:expect(rabl_amqp, connection_start, fun(_) ->
                                                     {ok, rabl_mock:mock_rabl_con(Pid)}
                                             end),
    meck:expect(rabl_amqp, connection_close, ['_'], meck:val(ok)),
    meck:expect(rabl_amqp, channel_open, ['_'], meck:val({error, channel_open_error})),

    {ok, Consumer} = start_link(test, ?AMQPURI),
    unlink(Consumer),
    Mon = monitor(process, Consumer),
    ?assertMatch(ok, meck:wait(5, rabl_amqp, connection_start, ['_'], 5000)),
    ?assertMatch(ok, meck:wait(5, rabl_amqp, channel_open, ['_'], 5000)),
    ?assertMatch(ok, meck:wait(5, rabl_amqp, connection_close, ['_'], 5000)),

    receive
        {'DOWN', Mon, process, Consumer, max_connection_retry_limit_reached} ->
            ok
    end,

    ?assertMatch(undefined, process_info(Consumer)),
    meck:unload(rabl_amqp),
    unmock_riak().

reopen_closed_connections() ->
    rabl_env_setup(),
    mock_riak(),
    meck:new(rabl_amqp, [passthrough]),
    meck:new(rabl_util, [passthrough]),
    Pid = self(),
    meck:expect(rabl_amqp, connection_start, fun(_) ->
                                                     {ok, rabl_mock:mock_rabl_con(Pid)}
                                             end),
    meck:expect(rabl_amqp, connection_close, ['_'], meck:val(ok)),
    meck:expect(rabl_amqp, channel_open, fun(_) ->
                                                 {ok, rabl_mock:mock_rabl_chan(Pid)}
                                         end),

    meck:expect(rabl_util, try_ensure_exchange, ['_', '_'], meck:val(ok)),
    meck:expect(rabl_amqp, set_prefetch_count, ['_', '_'], meck:val(ok)),
    meck:expect(rabl_amqp, subscribe, ['_', '_', '_'], meck:val({ok, <<"subscription">>})),

    {ok, Consumer} = start_link(test, ?AMQPURI),
    unlink(Consumer),
    Mon = monitor(process, Consumer),

    ?assertMatch(ok, meck:wait(1, rabl_amqp, connection_start, ['_'], 1000)),
    ?assertMatch(ok, meck:wait(1, rabl_amqp, channel_open, ['_'], 1000)),

    CurrentCon = rabl_mock:receive_connection(Pid),
    CurrentChan = rabl_mock:receive_channel(Pid),

    meck:reset(rabl_amqp),

    true = erlang:exit(CurrentCon, boom),

    ?assertMatch(ok, meck:wait(1, rabl_amqp, connection_start, ['_'], 1000)),
    ?assertMatch(ok, meck:wait(1, rabl_amqp, channel_open, ['_'], 1000)),

    NewCon = rabl_mock:receive_connection(Pid),
    NewChan = rabl_mock:receive_channel(Pid),

    ?assertNotEqual(CurrentCon, NewCon),
    ?assertNotEqual(CurrentChan, NewChan),

    receive
        {'DOWN', Mon, process, Consumer, max_connection_retry_limit_reached} ->
            ?assert(false)
    after 1000 -> ?assert(true)
    end,

    exit(Consumer, kill),

    meck:validate(rabl_amqp),
    meck:unload(rabl_amqp),
    meck:validate(rabl_util),
    meck:unload(rabl_util),
    unmock_riak().

reopen_closed_channels() ->
    rabl_env_setup(),
    mock_riak(),
    meck:new(rabl_amqp, [passthrough]),
    Pid = self(),
    meck:expect(rabl_amqp, connection_start, fun(_) ->
                                                     {ok, rabl_mock:mock_rabl_con(Pid)}
                                             end),

    meck:expect(rabl_amqp, channel_open, fun(_) ->
                                                 {ok, rabl_mock:mock_rabl_chan(Pid)}
                                         end),
    meck:expect(rabl_amqp, set_prefetch_count, ['_', '_'], meck:val(ok)),
    meck:expect(rabl_amqp, subscribe, ['_', '_', '_'], meck:val({ok, <<"subscription">>})),

    {ok, Consumer} = start_link(test, ?AMQPURI),
    unlink(Consumer),

    ?assertMatch(ok, meck:wait(1, rabl_amqp, connection_start, ['_'], 1000)),
    ?assertMatch(ok, meck:wait(1, rabl_amqp, channel_open, ['_'], 1000)),

    CurrentChan = rabl_mock:receive_channel(Pid),

    meck:reset(rabl_amqp),

    true = erlang:exit(CurrentChan, boom),

    ?assertMatch(ok, meck:wait(1, rabl_amqp, channel_open, ['_'], 1000)),

    NewChan = rabl_mock:receive_channel(Pid),

    ?assertNotEqual(CurrentChan, NewChan),

    exit(Consumer, kill),
    meck:unload(rabl_amqp),
    unmock_riak().

reopen_closed_channels_and_connections() ->
    rabl_env_setup(),
    mock_riak(),
    meck:new(rabl_amqp, [passthrough]),
    Pid = self(),

    meck:expect(rabl_amqp, connection_start, fun(_) ->
                                                     {ok, rabl_mock:mock_rabl_con(Pid)}
                                             end),
    meck:expect(rabl_amqp, connection_close, ['_'], meck:val(ok)),
    meck:expect(rabl_amqp, channel_open, fun(_) ->
                                                 {ok, rabl_mock:mock_rabl_chan(Pid)}
                                         end),
    meck:expect(rabl_amqp, set_prefetch_count, ['_', '_'], meck:val(ok)),
    meck:expect(rabl_amqp, subscribe, ['_', '_', '_'], meck:val({ok, <<"subscription">>})),

    {ok, Consumer} = start_link(test, ?AMQPURI),
    unlink(Consumer),

    ?assertMatch(ok, meck:wait(1, rabl_amqp, connection_start, ['_'], 1000)),
    ?assertMatch(ok, meck:wait(1, rabl_amqp, channel_open, ['_'], 1000)),

    CurrentCon = rabl_mock:receive_connection(Pid),
    CurrentChan = rabl_mock:receive_channel(Pid),

    meck:reset(rabl_amqp),

    %% fail a re-open on same connection
    meck:expect(rabl_amqp, channel_open, fun(Con) when Con == CurrentCon ->
                                                 {error, nope};
                                            (_) ->
                                                 {ok, rabl_mock:mock_rabl_chan(Pid)}
                                         end),

    true = erlang:exit(CurrentChan, boom),

    NewCon = rabl_mock:receive_connection(Pid),
    NewChan = rabl_mock:receive_channel(Pid),

    ?assertMatch(ok, meck:wait(1, rabl_amqp, channel_open, [CurrentCon], 1000)),
    ?assertMatch(ok, meck:wait(1, rabl_amqp, connection_start, ['_'], 1000)),
    ?assertMatch(ok, meck:wait(1, rabl_amqp, channel_open, ['_'], 1000)),

    ?assertNotEqual(CurrentCon, NewCon),
    ?assertNotEqual(CurrentChan, NewChan),

    exit(Consumer, kill),

    meck:unload(rabl_amqp),
    unmock_riak().

fail_on_max_subscribe_errors() ->
    rabl_env_setup(),
    mock_riak(),
    meck:new(rabl_amqp, [passthrough]),
    meck:new(rabl_util, [passthrough]),
    Pid = self(),
    meck:expect(rabl_amqp, connection_start, fun(_) ->
                                                     {ok, rabl_mock:mock_rabl_con(Pid)}
                                             end),
    meck:expect(rabl_amqp, connection_close, ['_'], meck:val(ok)),
    meck:expect(rabl_amqp, channel_open, fun(_) ->
                                                 {ok, rabl_mock:mock_rabl_chan(Pid)}
                                         end),
    meck:expect(rabl_amqp, channel_close, ['_'], meck:val(ok)),
    meck:expect(rabl_util, try_ensure_exchange, ['_', '_'], meck:val(ok)),
    meck:expect(rabl_amqp, set_prefetch_count, ['_', '_'], meck:val(ok)),
    meck:expect(rabl_amqp, subscribe, ['_', '_', '_'], meck:val({error, subscribe_fail})),

    {ok, Consumer} = start_link(test, ?AMQPURI),
    unlink(Consumer),
    Mon = monitor(process, Consumer),
    ?assertMatch(ok, meck:wait(1, rabl_amqp, connection_start, ['_'], 5000)),
    ?assertMatch(ok, meck:wait(1, rabl_amqp, channel_open, ['_'], 5000)),
    ?assertMatch(ok, meck:wait(5, rabl_amqp, subscribe, ['_', '_', '_'], 5000)),

    wait_for_message({'DOWN', Mon, process, Consumer, max_subscribe_retry_limit_reached}),

    ?assertMatch(undefined, process_info(Consumer)),
    meck:validate(rabl_amqp),
    meck:unload(rabl_amqp),
    meck:validate(rabl_util),
    meck:unload(rabl_util),
    unmock_riak().

subscribe_noproc_reconnects() ->
    rabl_env_setup(),
    mock_riak(),
    meck:new(rabl_amqp, [passthrough]),
    meck:new(rabl_util, [passthrough]),
    Pid = self(),
    meck:expect(rabl_amqp, connection_start, fun(_) ->
                                                     {ok, rabl_mock:mock_rabl_con(Pid)}
                                             end),
    meck:expect(rabl_amqp, connection_close, ['_'], meck:val(ok)),
    meck:expect(rabl_amqp, channel_open, fun(_) ->
                                                 {ok, rabl_mock:mock_rabl_chan(Pid)}
                                         end),
    meck:expect(rabl_amqp, channel_close, ['_'], meck:val(ok)),
    meck:expect(rabl_util, try_ensure_exchange, ['_', '_'], meck:val(ok)),
    meck:expect(rabl_amqp, set_prefetch_count, ['_', '_'], meck:val(ok)),
    meck:expect(rabl_amqp, subscribe, ['_', '_', '_'],
                meck:seq([
                          meck:raise(exit, {noproc,{gen_server,call, [sum_fun, 1]}}),
                          meck:val({ok, <<"subscription">>})
                         ])
               ),

    {ok, Consumer} = start_link(test, ?AMQPURI),
    unlink(Consumer),
    _Mon = monitor(process, Consumer),
    ?assertMatch(ok, meck:wait(2, rabl_amqp, connection_start, ['_'], 5000)),
    ?assertMatch(ok, meck:wait(2, rabl_amqp, channel_open, ['_'], 5000)),
    ?assertMatch(ok, meck:wait(2, rabl_amqp, subscribe, ['_', '_', '_'], 5000)),
    ?assertMatch(ok, meck:wait(1, rabl_amqp, connection_close, ['_'], 5000)),
    ?assertMatch(ok, meck:wait(1, rabl_amqp, channel_close, ['_'], 5000)),
    exit(Consumer, kill),
    meck:validate(rabl_amqp),
    meck:unload(rabl_amqp),
    meck:validate(rabl_util),
    meck:unload(rabl_util),
    unmock_riak().

%% Test that the handle_info for messages sent by rabbitmq does what
%% we expect with well known input
handles_rabbit_messages() ->
    rabl_env_setup(),
    mock_riak(),
    good_rabbit_mocks(),
    meck:new(rabl_stat),
    %% the match on binary_to_term/1 return needs to be respected,
    %% that's all
    {B, K}=BK = {<<"bucket">>, <<"key">>},
    TimingTag = <<"timing_tag">>,
    DummyObject =  <<"dummy_object">>,
    Tag = <<"tag">>,
    BinMsg = term_to_binary({TimingTag, BK, DummyObject}),
    MsgSeq = [
              {rabbit_msg, ok},
              {rabbit_msg, {msg, BinMsg, Tag}},
              {rabbit_msg, {msg, BinMsg, Tag}},
              {other, nonsense},
              {rabbit_msg, cancel} %% stops the consumer
             ],
    meck:expect(rabl_amqp, receive_msg, ['_'], meck:seq(MsgSeq)),

    meck:expect(rabl_stat, params_to_stat_name, ['_'], meck:val(mock_stat_name)),

    meck:expect(rabl_stat, consume, [mock_stat_name, '_', '_'], ok),
    meck:expect(rabl_stat, riak_put, [{[success, '_', '_'], ok},
                                      {[fail, '_', '_'], ok}]),
    meck:expect(rabl_riak, object_from_binary, [B, K, DummyObject], mock_riak_object),
    meck:expect(rabl_riak, client_put, [mock_client, mock_riak_object, ?RABL_PUT_OPTS],
                meck:seq([ok,
                          {error, overload}])
               ),

    {ok, Consumer} = start_link(test, ?AMQPURI),
    unlink(Consumer),
    Mon = monitor(process, Consumer),
    Channel = rabl_mock:receive_channel(self()),

    %% call expect later so we can use the actual channel in arg_spec
    meck:expect(rabl_amqp, ack_msg, [Channel, Tag], ok),
    meck:expect(rabl_amqp, nack_msg, [Channel, Tag], ok),

    %% send any N messages to trigger mecked rabl_amqp:receive_msg/1
    [Consumer ! N || N <- lists:seq(1, length(MsgSeq))],
    %% last message in is cancel, which stops the consumer
    wait_for_message({'DOWN', Mon, process, Consumer, subscription_cancelled}),
    ?assertMatch(undefined, process_info(Consumer)),
    ?assert(meck:validate(rabl_riak)),
    ?assert(meck:validate(rabl_amqp)),
    ?assert(meck:validate(rabl_stat)),
    meck:validate(rabl_amqp),
    meck:unload(rabl_amqp),
    meck:validate(rabl_util),
    meck:unload(rabl_util),
    meck:validate(rabl_stat),
    meck:unload(rabl_stat),
    unmock_riak().


wait_for_message(Msg) ->
    receive
        Msg ->
            ok;
        _Other ->
            wait_for_message(Msg)
    end.

%% those application variables that loading the app/config would
%% populate
rabl_env_setup() ->
    lager:start(),
    application:set_env(rabl, max_connection_retries, 5),
    application:set_env(rabl, connection_retry_delay_millis, 1000),
    application:set_env(rabl, sink_queue, <<"sink_queue">>).

mock_riak() ->
    meck:new(rabl_riak),
    meck:expect(rabl_riak, client_new, [], meck:val({ok, mock_client})).

%% sets up rabl_amqp mocks for the perfect path (connection, channel,
%% subscription)
good_rabbit_mocks() ->
    Pid = self(),
    meck:new(rabl_amqp, [passthrough]),
    meck:new(rabl_util, [passthrough]),
    meck:expect(rabl_amqp, connection_start, fun(_) ->
                                                     {ok, rabl_mock:mock_rabl_con(Pid)}
                                             end),
    meck:expect(rabl_amqp, connection_close, ['_'], meck:val(ok)),
    meck:expect(rabl_amqp, channel_open, fun(_) ->
                                                 {ok, rabl_mock:mock_rabl_chan(Pid)}
                                         end),
    meck:expect(rabl_amqp, channel_close, ['_'], meck:val(ok)),
    meck:expect(rabl_amqp, set_prefetch_count, ['_', '_'], meck:val(ok)),
    meck:expect(rabl_util, try_ensure_exchange, ['_', '_'], meck:val(ok)),
    meck:expect(rabl_amqp, subscribe, ['_', '_', '_'], meck:val({ok, <<"subscription">>})).

flush() ->
    receive
        _ -> flush()
    after
        0 -> ok
    end.

unmock_riak() ->
    flush(),
    meck:unload(rabl_riak).

-endif.
