%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 20 Apr 2017 by Russell Brown <russell@wombat.me>

-module(rabl_hook).

-compile(export_all).

-include_lib("amqp_client/include/amqp_client.hrl").

-define(BUCKET, <<"rabl">>).
-define(RABL_PG, rabl).

-spec connect() -> pid().
connect() ->
    {ok, Connection} = amqp_connection:start(#amqp_params_network{}),
    Connection.

-spec disconnect(pid()) -> ok.
disconnect(Connection) when is_pid(Connection) ->
    amqp_connection:close(Connection).

-spec open_channel(pid()) -> pid().
open_channel(Connection) when is_pid(Connection) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),
    Channel.

-spec close_channel(Channel::pid()) -> ok.
close_channel(Channel) when is_pid(Channel) ->
    amqp_channel:close(Channel),
    ok.

-spec make_queue(pid(), binary()) -> binary().
make_queue(Channel, Name) when is_binary(Name), is_pid(Channel) ->
    Declare = #'queue.declare'{queue = Name},
    #'queue.declare_ok'{} = amqp_channel:call(Channel, Declare),
    Name.

-spec del_queue(Channel::pid(), Queue::binary()) -> ok.
del_queue(Channel, Queue) when is_pid(Channel),
                               is_binary(Queue) ->
    Delete = #'queue.delete'{queue = Queue},
    #'queue.delete_ok'{} = amqp_channel:call(Channel, Delete),
    ok.

-spec make_exchange(pid(), binary()) -> binary().
make_exchange(Channel, Name) when is_binary(Name), is_pid(Channel) ->
    Declare = #'exchange.declare'{exchange = Name},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, Declare),
    Name.

-spec del_exchange(Channel::pid(), Exchange::binary()) -> ok.
del_exchange(Channel, Exchange) when is_pid(Channel),
                                     is_binary(Exchange) ->
    Delete = #'exchange.delete'{exchange = Exchange},
    #'exchange.delete_ok'{} = amqp_channel:call(Channel, Delete),
    ok.

-spec bind(Channel::pid(), binary(), binary(), RoutingKey::binary()) ->
                  RoutingKey::binary().
bind(Channel, Exchange, Queue, RoutingKey) when is_pid(Channel),
                                                is_binary(Exchange),
                                                is_binary(Queue),
                                                is_binary(RoutingKey) ->
    Binding = #'queue.bind'{queue = Queue,
                            exchange = Exchange,
                            routing_key = RoutingKey},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
    RoutingKey.

-spec unbind(Channel::pid(),
             Exchange::binary(),
             Queue::binary(),
             RoutingKey::binary()) ->
                    ok.
unbind(Channel, Exchange, Queue, RoutingKey) when is_pid(Channel),
                                                  is_binary(Exchange),
                                                  is_binary(Queue),
                                                  is_binary(RoutingKey) ->
    Binding = #'queue.unbind'{queue       = Queue,
                              exchange    = Exchange,
                              routing_key = RoutingKey},
    #'queue.unbind_ok'{} = amqp_channel:call(Channel, Binding),
    ok.


-spec publish(pid(), binary(), binary(), binary()) -> ok.
publish(Channel, Exchange, RoutingKey, Message) when is_pid(Channel),
                                                     is_binary(Exchange),
                                                     is_binary(RoutingKey),
                                                     is_binary(Message) ->
    Publish = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
    amqp_channel:cast(Channel, Publish, #amqp_msg{payload = Message}),
    ok.

-spec subscribe(pid(), binary()) -> Tag::term().
subscribe(Channel, Queue) when is_pid(Channel),
                               is_binary(Queue) ->
    Consumer = spawn(?MODULE, loop, [Channel]),
    Subscription = #'basic.consume'{queue = Queue},
    #'basic.consume_ok'{consumer_tag = Tag} = amqp_channel:subscribe(Channel, Subscription, Consumer),
    Tag.

-spec unsubscribe(pid(), Tag::term()) -> ok.
unsubscribe(Channel, Tag) when is_pid(Channel) ->
    amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = Tag}),
    ok.

%% message accept loop
-spec loop(pid()) -> ok.
loop(Channel) ->
    receive
        %% This is the first message received
        #'basic.consume_ok'{} ->
            loop(Channel);

        %% This is received when the subscription is cancelled
        #'basic.cancel_ok'{} ->
            ok;

        %% A delivery
        {#'basic.deliver'{delivery_tag = Tag}, #'amqp_msg'{payload=Content}} ->
            %% Do something with the message payload
            %% (some work here)
            io:format("got message ~p with tag ~p~n", [Content, Tag]),
            %% Ack the message
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),

            %% Loop
            loop(Channel)
    end.

smoke_test() ->
    Connection = connect(),
    io:format("connected~n"),
    Channel = open_channel(Connection),
    io:format("got channel~n"),
    Queue = make_queue(Channel, <<"rdb-1">>),
    io:format("made queue~n"),
    Exchange = make_exchange(Channel, <<"rdb-ex">>),
    io:format("made exchange~n"),
    RoutingKey = bind(Channel, Exchange, Queue, <<"rdb-rk">>),
    io:format("bound and got routing key~n"),
    Tag = subscribe(Channel, Queue),
    io:format("subscribed with tag ~p~n", [Tag]),
    Msg = <<"hello-world!">>,
    publish(Channel, Exchange, RoutingKey, Msg),
    io:format("sent message ~p~n", [Msg]),
    %% @TODO wait for message, not for a time
    timer:sleep(1000),
    %% tear it all down
    ok = unsubscribe(Channel, Tag),
    io:format("unsubscribed~n"),
    ok = unbind(Channel, Exchange, Queue, RoutingKey),
    io:format("unbound~n"),
    ok = del_exchange(Channel, Exchange),
    io:format("deleted exchange~n"),
    ok = del_queue(Channel, Queue),
    io:format("deleted queue~n"),
    ok = close_channel(Channel),
    io:format("closed channel~n"),
    ok = disconnect(Connection),
    io:format("disconnected~n").

%% QAD set up, returns the name of the queue that the _other_
%% cluster(s) should subscribe to to get replicated objects
-spec setup_rabl() -> Queue::binary().
setup_rabl() ->
    NodeBin = atom_to_binary(node(), utf8),
    Connection = connect(),
    io:format("connected~n"),
    Channel = open_channel(Connection),
    io:format("got channel~n"),
    Queue = make_queue(Channel, NodeBin),
    io:format("made queue~n"),
    Exchange = make_exchange(Channel, NodeBin),
    io:format("made exchange~n"),
    bind(Channel, Exchange, Queue, NodeBin),
    io:format("queue, exchange, routing key set up ~p~n", [NodeBin]),
    pg2:join(?RABL_PG, Channel),
    Queue.

%% QAD add hook
-spec add_hook() -> ok.
add_hook() ->
    {ok, C} = riak:local_client(),
    C:set_bucket(?BUCKET,[{postcommit, [{struct,[{<<"mod">>,<<"rabl_hook">>},{<<"fun">>, <<"rablicate">>}]}]}]),
    riak_core_bucket:get_bucket(?BUCKET).

%% convenience local put
-spec put(Key::binary(), Val::binary()) -> ok.
put(Key, Value) ->
    {ok, C} = riak:local_client(),
    Obj = riak_object:new(?BUCKET, Key, Value),
    ok = C:put(Obj, []),
    ok.

%% close all channels in PG (what about connection(s)?)
teardown_rabl() ->
    NodeBin = atom_to_binary(node(), utf8),
    case pg2:get_members(?RABL_PG) of
        {error, _Meh} -> ok;
        Pids when is_list(Pids) ->
            lists:foreach(fun(Channel) ->
                                  ok = unbind(Channel, NodeBin, NodeBin, NodeBin),
                                  ok = del_exchange(Channel, NodeBin),
                                  ok = del_queue(Channel, NodeBin),
                                  ok = close_channel(Channel),
                                  %% TODO erm, how to disconnect??)
                                  ok
                          end,
                          Pids)
    end.

%% Quick And Dirty test hook, expects that all the exhange etc etc are
%% all set up in advance
rablicate(Object) ->
    io:format("hook called~n"),
    NodeBin = atom_to_binary(node(), utf8),
    {ok, Channel} = get_channel(),
    %% TODO add B/K info, to binary for other end.
    BK = {riak_object:bucket(Object), riak_object:key(Object)},
    BinObj = riak_object:to_binary(v1, Object),
    Msg = term_to_binary({BK, BinObj}),
    io:format("rablicating ~p~n", [BK]),
    publish(Channel, NodeBin, NodeBin, Msg).

%% the repl receiver subscription
%% TODO what about unsubscribing/tag state
rabl_subscribe(Queue) ->
    {ok, Channel} = get_channel(),
    {ok, Client} = riak:local_client(),
    Consumer = spawn(?MODULE, rabl_consume, [Channel, Client]),
    Subscription = #'basic.consume'{queue = Queue},
    #'basic.consume_ok'{consumer_tag = Tag} = amqp_channel:subscribe(Channel, Subscription, Consumer),
    Tag.

rabl_unsubscribe(Tag) ->
    {ok, Channel} = get_channel(),
    unsubscribe(Channel, Tag).

%% QAD consumer, uses local client, eh!
-spec rabl_consume(Channel::pid(), riak_client:riak_client()) -> ok.
rabl_consume(Channel, Client) ->
    receive
        %% This is the first message received
        #'basic.consume_ok'{} ->
            rabl_consume(Channel, Client);

        %% This is received when the subscription is cancelled
        #'basic.cancel_ok'{} ->
            ok;

        %% A delivery
        {#'basic.deliver'{delivery_tag = Tag}, #'amqp_msg'{payload=Content}} ->
            io:format("got message with tag ~p~n", [Tag]),
            {{B, K}, BinObj} = binary_to_term(Content),
            Obj = riak_object:from_binary(B, K, BinObj),
            io:format("putting ~p ~p~n", [B, K]),
            ok = Client:put(Obj, [asis, disable_hooks]),
            %% Ack the message
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),

            rabl_consume(Channel, Client)
    end.

%% TODO better than pg2, please
-spec get_channel() -> {ok, pid()}.
get_channel() ->
    case pg2:get_closest_pid(?RABL_PG) of
        {error, {no_such_group, ?RABL_PG}} ->
            pg2:create(?RABL_PG),
            get_channel();
        {error, {no_process, ?RABL_PG}} ->
            start_channel();
        Pid ->
            {ok, Pid}
    end.

%% start and "register" in pg2, assumes exchange, q, binding all exist
-spec start_channel() -> {ok, pid()} | {error, Reason::term()}.
start_channel() ->
    Conn = connect(),
    Channel = open_channel(Conn),
    pg2:join(?RABL_PG, Channel),
    {ok, Channel}.


