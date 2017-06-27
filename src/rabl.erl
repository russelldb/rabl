%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created :  2 May 2017 by Russell Brown <russell@wombat.me>

-module(rabl).

-compile(export_all).

-include_lib("amqp_client/include/amqp_client.hrl").

-spec connect() -> pid().  connect() -> {ok, Connection} =
amqp_connection:start(#amqp_params_network{}), Connection.

-spec connect(Host::string()) -> pid().
connect(Host) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_network{host=Host}),
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

-spec get_channel() -> {ok, pid()}.
get_channel() ->
    rabl_channel:get().

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



-spec subscribe(Channel::pid(), Queue::binary(),
                Mod::module(), State::term()) -> Tag::binary().
subscribe(Channel, Queue, Mod, State) when is_pid(Channel),
                               is_binary(Queue) ->
    Consumer = spawn(?MODULE, consume, [Channel, Mod, State]),
    subscribe(Channel, Queue, Consumer).

%% @doc subscribe for an existing process that will handle recieves
%% itself
-spec subscribe(Channel::pid(), Queue::binary(), Consumer::pid()) -> Tag::binary().
subscribe(Channel, Queue, Consumer) ->
    Subscription = #'basic.consume'{queue = Queue},
    #'basic.consume_ok'{consumer_tag = Tag} = amqp_channel:subscribe(Channel, Subscription, Consumer),
    Tag.

-spec unsubscribe(pid(), Tag::term()) -> ok.
unsubscribe(Channel, Tag) when is_pid(Channel) ->
    amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = Tag}),
    ok.

%% basic consume loop that callsback Mod:on_message(Message, State).
-spec consume(Channel::binary(), Mod::module(), State::term()) -> ok.
consume(Channel, Mod, State) ->
    receive
        %% This is the first message received
        #'basic.consume_ok'{} ->
            consume(Channel, Mod, State);

        %% This is received when the subscription is cancelled
        #'basic.cancel_ok'{} ->
            ok;

        %% A delivery
        {#'basic.deliver'{delivery_tag = Tag}, #'amqp_msg'{payload=Content}} ->
            State2 = Mod:on_message(Content, State),
            %% Ack the message
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
            consume(Channel, Mod, State2)
    end.


%% @doc receive_msg encapsulate rabbitmq specific matching
-spec receive_msg(Msg::term()) -> {rabbit_msg, ok} |
                                  {rabbit_msg, cancel} |
                                  {rabbit_msg, {msg, Content::binary(), Tag::binary()}} |
                                  {other, Msg::term()}.
receive_msg(#'basic.consume_ok'{}) ->
    {rabbit_msg, ok};
receive_msg(#'basic.cancel_ok'{}) ->
    {rabbit_msg, cancel};
receive_msg({#'basic.deliver'{delivery_tag = Tag}, #'amqp_msg'{payload=Content}}) ->
    {rabbit_msg, {msg, Content, Tag}};
receive_msg(Other) ->
    {other, Other}.

-spec ack_msg(Channel::pid(), Tag::binary()) -> ok.
ack_msg(Channel, Tag) ->
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}).

