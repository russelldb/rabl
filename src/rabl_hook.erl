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
%% @see rabl_helper:setup_rabl/0
%% @see rabl_helper:add_hook/1
rablicate(Object) ->
    io:format("hook called~n"),
    NodeBin = atom_to_binary(node(), utf8),
    {ok, Channel} = rabl:get_channel(),
    %% TODO add B/K info, to binary for other end.
    BK = {riak_object:bucket(Object), riak_object:key(Object)},
    BinObj = riak_object:to_binary(v1, Object),
    Msg = term_to_binary({BK, BinObj}),
    io:format("rablicating ~p~n", [BK]),
    rabl:publish(Channel, NodeBin, NodeBin, Msg).
