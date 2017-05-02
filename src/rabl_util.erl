%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created :  2 May 2017 by Russell Brown <russell@wombat.me>
-module(rabl_util).

-compile(export_all).

%% QAD set up, returns the name of the queue that the _other_
%% cluster(s) should subscribe to to get replicated objects
-spec setup_rabl() -> Queue::binary().
setup_rabl() ->
    NodeBin = atom_to_binary(node(), utf8),
    Connection = rabl:connect(),
    io:format("connected~n"),
    Channel = rabl:open_channel(Connection),
    io:format("got channel~n"),
    Queue = rabl:make_queue(Channel, NodeBin),
    io:format("made queue~n"),
    Exchange = rabl:make_exchange(Channel, NodeBin),
    io:format("made exchange~n"),
    rabl:bind(Channel, Exchange, Queue, NodeBin),
    io:format("queue, exchange, routing key set up ~p~n", [NodeBin]),
    rabl_channel:add(Channel),
    Queue.

%% QAD add hook
-spec add_hook(Bucket::binary() | {binary(), binary()}) -> ok.
add_hook(Bucket) ->
    {ok, C} = riak:local_client(),
    C:set_bucket(Bucket,[{postcommit, [{struct,[{<<"mod">>,<<"rabl_hook">>},{<<"fun">>, <<"rablicate">>}]}]}]),
    riak_core_bucket:get_bucket(Bucket).

%% convenience local put
-spec put(Bucket::binary() | {binary(), binary()},
          Key::binary(), Val::binary()) -> ok.
put(Bucket, Key, Value) ->
    {ok, C} = riak:local_client(),
    Obj = riak_object:new(Bucket, Key, Value),
    ok = C:put(Obj, []),
    ok.

%% close all channels in PG (what about connection(s)?)
teardown_rabl() ->
    NodeBin = atom_to_binary(node(), utf8),
    Pids = rabl_channel:get_all(),
    lists:foreach(fun(Channel) ->
                          ok = rabl:unbind(Channel, NodeBin, NodeBin, NodeBin),
                          ok = rabl:del_exchange(Channel, NodeBin),
                          ok = rabl:del_queue(Channel, NodeBin),
                          ok = rabl_channel:close(Channel),
                          %% TODO erm, how to disconnect??)
                          ok
                  end,
                  Pids).

%% the repl receiver subscription
%% TODO what about unsubscribing/tag state
subscribe(Queue) ->
    {ok, Channel} = rabl_channel:get(),
    {ok, Client} = riak:local_client(),
    rabl:subscribe(Channel, Queue, ?MODULE, Client).

unsubscribe(Tag) ->
    {ok, Channel} = rabl_channel:get(),
    rabl:unsubscribe(Channel, Tag).

%% QAD consumer, uses local client, eh!
-spec on_message(Content::binary(), State::term()) -> State::term().
on_message(Message, Client) ->
    {{B, K}, BinObj} = binary_to_term(Message),
    Obj = riak_object:from_binary(B, K, BinObj),
    io:format("putting ~p ~p~n", [B, K]),
    ok = Client:put(Obj, [asis, disable_hooks]),
    Client.

