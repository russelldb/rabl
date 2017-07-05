%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%
%%% riak post commit hook that sends an object to the configured
%%% rabbitmq queue. NOTE: Expects an exchange and routing key named
%%% for the cluster to be configured. The application environment
%%% setting {rabl, cluster_name} should be the name of a queue and
%%% exchange that are set up on rabbitmq for this hook to publish to
%%% as the source of changes to replicate.
%%
%%% @end
%%
%%% @see rabl_util for helper functions to set up queue etc. There
%%% should be operator commands in rabl_escript
%%
%%% Created : 20 Apr 2017 by Russell Brown
%%% <russell@wombat.me>

-module(rabl_hook).

-compile(export_all).

%% we don't depend on riak_kv, but can only be run inside a riak node
-ignore_xref({riak_object, bucket, 1}).
-ignore_xref({riak_object, key, 1}).
-ignore_xref({riak_object, to_binary, 2}).

%% Simpliest thing that could work.
-spec rablicate(riak_object:riak_object()) -> ok |
                                              {fail, Reason::term()}.
rablicate(Object) ->
    lager:debug("rabl hook called~n"),
    BK = {riak_object:bucket(Object), riak_object:key(Object)},
    BinObj = riak_object:to_binary(v1, Object),
    Time = os:timestamp(),
    %% We want to match up remotes, but don't want remote pid prefix
    %% in remote log, so don't _be_ a pid.
    Tag = {Time, node(), erlang:pid_to_list(self())},
    Msg = term_to_binary({Tag, BK, BinObj}),
    lager:debug("rablicating ~p~n", [BK]),
    Res = case rabl_producer:publish(Msg) of
              ok ->
                  rabl_stat:publish(BK, Tag, Time),
                  ok;
              Error ->
                  rabl_stat:publish_fail(BK, Tag, Time, Error),
                  {fail, Error}
          end,
    Res.
