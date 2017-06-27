%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%% a bridge between rabl and amqp, so we can test more easily and
%%% encaspulate anything that depends on the 3rd party amqp libs.
%%% @end
%%% Created : 15 Jun 2017 by Russell Brown <russell@wombat.me>

-module(rabl_amqp).

-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-export_type([amqp_connection_params/0, rabbit_message/0]).

-opaque amqp_connection_params() :: #amqp_params_network{} | #amqp_params_direct{}.
-opaque rabbit_message() :: #'basic.consume_ok'{} | #'basic.cancel_ok'{} | #'basic.deliver'{}.

%% @doc parse the given `AMQPURI' into an opaque term that can be
%% passed back to `connection_start/1'
-spec parse_uri(string()) -> {ok, amqp_connection_params()}
                                 | {error, Reason::term()}.
parse_uri(AMQPURI) ->
    amqp_uri:parse(AMQPURI).

%% @doc connection_start:: start a rabbimq amqp connection, returns a
%% pid() or error.
-spec connection_start(amqp_connection_params()) -> {ok, Connection::pid()}
                                                        | {error, Error::term()}.
connection_start(AMQPParams=#amqp_params_network{}) ->
    amqp_connection:start(AMQPParams);
connection_start(AMQPParams=#amqp_params_direct{}) ->
    amqp_connection:start(AMQPParams).

%% @doc connection_close:: close a rabbitmq connection.
-spec connection_close(Connection::pid()) -> ok.
connection_close(Connection) when is_pid(Connection) ->
    amqp_connection:close(Connection).

%% @doc open a channel on the given `Connection'
-spec channel_open(Connection::pid()) -> {ok, Channel::pid()}
                                             | {error, Error::term()}.
channel_open(Connection) when is_pid(Connection) ->
    amqp_connection:open_channel(Connection).

%% @doc closes a channel.
-spec channel_close(Channel::pid()) -> ok.
channel_close(Channel) when is_pid(Channel) ->
    amqp_channel:close(Channel).

%% @doc set the prefetch count on a channel
-spec set_prefetch_count(Channel::pid(), Prefetch::pos_integer()) -> ok.
set_prefetch_count(Channel, Prefetch) ->
    QOS = #'basic.qos'{prefetch_count = Prefetch},
    #'basic.qos_ok'{} = amqp_channel:call(Channel, QOS),
    ok.

%% @doc subscribe `Subscriber' pid() to `Queue' on `Channel'. Returns
%% an opaque `Tag' for later cancelling subscription.
-spec subscribe(Channel::pid(), Queue::binary(), Subscriber::pid()) ->
    {ok, Tag::binary()} | {error, Error::term()}.
subscribe(Channel, Queue, Subscriber) when is_pid(Channel),
                                           is_binary(Queue),
                                           is_pid(Subscriber) ->
    Subscription = #'basic.consume'{queue = Queue},
    case amqp_channel:subscribe(Channel, Subscription, Subscriber) of
        #'basic.consume_ok'{consumer_tag = Tag} -> {ok, Tag};
        Error -> {error, Error}
    end.

%% @doc receive_msg encapsulate rabbitmq specific matching
-spec receive_msg(Msg::rabbit_message()) -> {rabbit_msg, ok} |
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
ack_msg(Channel, Tag) when is_pid(Channel), is_binary(Tag) ->
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}).

-spec nack_msg(Channel::pid(), Tag::binary()) -> ok.
nack_msg(Channel, Tag) when is_pid(Channel), is_binary(Tag) ->
    %% NOTE: made defaults explicit in case they change in future, and
    %% to save time googling docs. This means that only message Tag is
    %% rejected (not those up-to-and-including Tag) and that the
    %% message is reque'd for redelivery to a different consumer
    amqp_channel:cast(Channel, #'basic.nack'{delivery_tag = Tag,
                                             multiple=false,
                                             requeue=true}).