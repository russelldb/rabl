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
    {ok, C} = rabl_riak_client:new(),
    rabl_riak_client:set_bucket(C, Bucket,[{postcommit, [{struct,[{<<"mod">>,<<"rabl_hook">>},{<<"fun">>, <<"rablicate">>}]}]}]).

%% convenience local put
-spec put(Bucket::binary() | {binary(), binary()},
          Key::binary(), Val::binary()) -> ok.
put(Bucket, Key, Value) ->
    {ok, C} = rabl_riak_client:new(),
    %% @TODO: dependancy on riak_kv/riak_object
    Obj = riak_object:new(Bucket, Key, Value),
    ok = rabl_riak_client:put(C, Obj, []),
    ok.

%% the repl receiver subscription
%% TODO what about unsubscribing/tag state
-spec subscribe(pid()) -> binary().
subscribe(Queue) ->
    {ok, Channel} = rabl_channel:get(),
    {ok, Client} = rabl_riak_client:new(),
    rabl:subscribe(Channel, Queue, ?MODULE, Client).

-spec unsubscribe(binary()) -> ok.
unsubscribe(Tag) ->
    {ok, Channel} = rabl_channel:get(),
    rabl:unsubscribe(Channel, Tag).

%% Consumer fun NOTE: @TODO what about depending on riak_kv here for
%% riak_object? Maybe better to use HTTP/PB/public API
-spec on_message(Content::binary(), State::term()) -> State::term().
on_message(Message, Client) ->
    {{B, K}, BinObj} = binary_to_term(Message),
    Obj = riak_object:from_binary(B, K, BinObj),
    lager:debug("putting ~p ~p~n", [B, K]),
    ok = rabl_riak_client:put(Client, Obj, [asis, disable_hooks]),
    Client.
