%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 20 Apr 2017 by Russell Brown <russell@wombat.me>

-module(rabl_hook).

-compile(export_all).

%% Simpliest thing that could work. Expects an exchange and routing
%% key named for the cluster.
-spec rablicate(riak_object:riak_object()) -> ok |
                                              {fail, Reason::term()}.
rablicate(Object) ->
    lager:debug("hook called~n"),
    {ok, Cluster} = application:get_env(rabl, cluster_name),
    {ok, Channel} = rabl:get_channel(),
    BK = {riak_object:bucket(Object), riak_object:key(Object)},
    BinObj = riak_object:to_binary(v1, Object),
    Time = os:timestamp(),
    %% We want to match up remotes, but don't want remote pid prefix
    %% in remote log, so don't _be_ a pid.
    Tag = {Time, node(), erlang:pid_to_list(self())},
    Msg = term_to_binary({Tag, BK, BinObj}),
    lager:debug("rablicating ~p~n", [BK]),
    Res = case rabl:publish(Channel, Cluster, Cluster, Msg) of
              ok ->
                  rabl_stat:publish(BK, Tag, Time),
                  ok;
              Error ->
                  {fail, Error}
          end,
    Res.
