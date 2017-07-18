%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created :  2 May 2017 by Russell Brown <russell@wombat.me>
-module(rabl_util).

-compile(export_all).

%% QAD add hook
-spec add_hook(Bucket::binary() | {binary(), binary()}) -> ok.
add_hook(Bucket) ->
    {ok, C} = rabl_riak:client_new(),
    rabl_riak:set_bucket(C, Bucket,[{postcommit, [{struct,[{<<"mod">>,<<"rabl_hook">>},{<<"fun">>, <<"rablicate">>}]}]}]).

%% convenience local put
-spec put(Bucket::binary() | {binary(), binary()},
          Key::binary(), Val::binary()) -> ok.
put(Bucket, Key, Value) ->
    {ok, C} = rabl_riak:client_new(),
    %% @TODO: dependancy on riak_kv/riak_object
    Obj = riak_object:new(Bucket, Key, Value),
    ok = rabl_riak:client_put(C, Obj, []),
    ok.

%% sets up exchange, queue, bindings locally for a smoke test NOTE:
%% run before starting the app.
setup_local_smoke() ->
    application:load(rabl),
    RablEnv = application:get_all_env(rabl),
    ClusterName = proplists:get_value(cluster_name, RablEnv),
    SinkQueue = proplists:get_value(sink_queue, RablEnv),
    Producers = proplists:get_value(producers, RablEnv),
    Consumers = proplists:get_value(consumers, RablEnv),
    ok = setup_queues(ClusterName, Producers),
    ok = setup_queues(SinkQueue, Consumers).

setup_queues(_Queue, []) ->
    ok;
setup_queues(Queue, [{_Cnt, URI} | Rest]) ->
    setup_queue(Queue, URI),
    setup_queues(Queue, Rest).

setup_queue(Queue, AMQPURI) ->
    {ok, AMQPParams} = rabl_amqp:parse_uri(AMQPURI),
    {ok, Connection}  = rabl_amqp:connection_start(AMQPParams),
    io:format("connected~n"),
    {ok, Channel} = rabl_amqp:channel_open(Connection),
    io:format("got channel~n"),
    {ok, Queue} = rabl_amqp:queue_create(Channel, Queue),
    io:format("made queue~n"),
    {ok, Exchange} = rabl_amqp:exchange_create(Channel, Queue),
    io:format("made exchange~n"),
    {ok, RoutingKey} = rabl_amqp:bind(Channel, Exchange, Queue, Queue),
    RoutingKey.

