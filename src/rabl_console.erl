%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%% console funs
%%% @end
%%% Created : 18 Jul 2017 by Russell Brown <russell@wombat.me>

-module(rabl_console).

-compile(export_all).

-spec show_config(list()) -> proplists:proplist().
show_config([]) ->
    io:format("rabl env: ~p~n", [application:get_all_env(rabl)]).

-spec add_hook([binary()]) -> proplists:proplist().
add_hook([Bucket]) ->
    BinBucket = list_to_binary(Bucket),
    io:format("adding rabl_hook to bucket ~p~n", [BinBucket]),
    rabl_util:add_hook(BinBucket),
    BucketProps = rabl_util:get_bucket(BinBucket),
    io:format("ok: ~p~n", [BucketProps]).

-spec load([]) -> ok.
load([]) ->
    ok = rabl_util:load(),
    io:format("ok~n",[]).

-spec start([]) -> ok.
start([]) ->
    io:format("Starting rabl app~n", []),
    Res = rabl_app:start(),
    io:format("~p~n", [Res]).

-spec setup([]) -> ok.
setup([]) ->
    io:format("Setting up queues from local rabl config~n", []),
    rabl_util:setup_local_smoke().

-spec stop([]) -> ok.
stop([]) ->
    io:format("Stopping rabl app~n", []),
    Res = application:stop(rabl),
    io:format("~p~n", [Res]).

-spec status([]) -> ok.
status([]) ->
    Status = rabl_app:status(),
    io:format("~p~n", [Status]).

-spec stats([]) -> ok.
stats([]) ->
    {ok, Stats} = rabl_stat:get_stats(),
    io:format("~p~n", [Stats]).

