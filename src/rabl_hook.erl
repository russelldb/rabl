%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 20 Apr 2017 by Russell Brown <russell@wombat.me>

-module(rabl_hook).

-compile(export_all).

%% Quick And Dirty test hook, expects that all the exhange etc etc are
%% all set up in advance.
%% @see rabl_util:setup_rabl/0
%% @see rabl_util:add_hook/1
rablicate(Object) ->
    lager:debug("hook called~n"),
    Cluster = app_helper:get_env(riak_core, cluster_name),
    ClusterBin = term_to_binary(Cluster),
    {ok, Channel} = rabl:get_channel(),
    BK = {riak_object:bucket(Object), riak_object:key(Object)},
    BinObj = riak_object:to_binary(v1, Object),
    Msg = term_to_binary({BK, BinObj}),
    lager:debug("rablicating ~p~n", [BK]),
    rabl:publish(Channel, ClusterBin, ClusterBin, Msg).
