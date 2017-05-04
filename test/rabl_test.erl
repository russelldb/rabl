%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created :  2 May 2017 by Russell Brown <russell@wombat.me>

-module(rabl_test).

-compile(export_all).

smoke_test() ->
    Connection = rabl:connect(),
    io:format("connected~n"),
    Channel = rabl:open_channel(Connection),
    io:format("got channel~n"),
    Queue = rabl:make_queue(Channel, <<"rdb-1">>),
    io:format("made queue~n"),
    Exchange = rabl:make_exchange(Channel, <<"rdb-ex">>),
    io:format("made exchange~n"),
    RoutingKey = rabl:bind(Channel, Exchange, Queue, <<"rdb-rk">>),
    io:format("bound and got routing key~n"),
    Tag = rabl:subscribe(Channel, Queue, ?MODULE, ok),
    io:format("subscribed with tag ~p~n", [Tag]),
    Msg = <<"hello-world!">>,
    rabl:publish(Channel, Exchange, RoutingKey, Msg),
    io:format("sent message ~p~n", [Msg]),
    %% @TODO wait for message, not for a time
    timer:sleep(1000),
    %% tear it all down
    ok = rabl:unsubscribe(Channel, Tag),
    io:format("unsubscribed~n"),
    ok = rabl:unbind(Channel, Exchange, Queue, RoutingKey),
    io:format("unbound~n"),
    ok = rabl:del_exchange(Channel, Exchange),
    io:format("deleted exchange~n"),
    ok = rabl:del_queue(Channel, Queue),
    io:format("deleted queue~n"),
    ok = rabl:close_channel(Channel),
    io:format("closed channel~n"),
    ok = rabl:disconnect(Connection),
    io:format("disconnected~n").

on_message(Message, ok) ->
    io:format("Got message ~p~n", [Message]),
    ok.
