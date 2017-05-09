%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%% wrap a riak client, so we can change transport etc
%%% @end
%%% Created :  9 May 2017 by Russell Brown <russell@wombat.me>

-module(rabl_riak_client).

-compile(export_all).

-export_type([client/0]).

-opaque client() :: riak_client:riak_client().

%% create a new client. Uses app config to connect. Returns `{ok, Client}'
%% where C is an opaque term to be used for other functions.
-spec new() -> client().
new() ->
    RiakNode = application:get_env(rabl, riak_node),
    {ok, C} = riak:client_connect(RiakNode),
    {ok, C}.

%% put a riak_object
-spec put(client(), riak_object:riak_object(), list()) -> ok | {error, Reason::term()}.
put(Client, Object, PutOptions) ->
    Client:put(Object, PutOptions).

-spec set_bucket(client(), Bucket::binary(), Props::list()) -> ok.
set_bucket(Client, Bucket, Props) ->
    ok = Client:set_bucket(Bucket, Props),
    ok.



