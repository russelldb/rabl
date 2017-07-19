%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%% wrap a riak client, so we can change transport etc
%%% @end
%%% Created :  9 May 2017 by Russell Brown <russell@wombat.me>

-module(rabl_riak).

-compile(export_all).

-export_type([client/0, object/0]).

%% we don't depend on riak_kv, but can only be run inside a riak node
-ignore_xref({riak, local_client, 0}).
-ignore_xref({riak_object, from_binary, 3}).

-opaque client() :: riak_client:riak_client().
-opaque object() :: riak_object:riak_object().
-type bucket() :: riak_object:bucket().
-type key() :: riak_object:key().

%% @doc create a new client. Uses app config to connect. Returns `{ok,
%% Client}' where C is an opaque term to be used for other functions.
-spec client_new() -> {ok, client()}.
client_new() ->
    {ok, C} = riak:local_client(),
    {ok, C}.

%% @doc put a riak_object, `Client' is obtained from `client_new/0' above.
-spec client_put(client(), riak_object:riak_object(), list()) -> ok | {error, Reason::term()}.
client_put(Client, Object, PutOptions) ->
    Client:put(Object, PutOptions).

%% @doc set bucket properties `Props' on `Bucket' using `Client'
%% obtained from `client_new/0' above.
-spec set_bucket(client(), Bucket::binary(), Props::list()) -> ok.
set_bucket(Client, Bucket, Props) ->
    ok = Client:set_bucket(Bucket, Props),
    ok.

%% @doc get bucket props.
-spec get_bucket(client(), Bucket::binary()) -> proplists:proplist().
get_bucket(Client, Bucket) ->
    Client:get_bucket(Bucket).

%% @doc bridge to riak_object code, take a riak object binary and
%% decode it.
-spec object_from_binary(bucket(), key(), binary()) -> object().
object_from_binary(B, K, BinObj) when is_binary(BinObj) ->
    riak_object:from_binary(B, K, BinObj).


