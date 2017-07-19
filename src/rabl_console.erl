%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%% console funs
%%% @end
%%% Created : 18 Jul 2017 by Russell Brown <russell@wombat.me>

-module(rabl_console).

-compile(export_all).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
    FlattendStats = flatten_stats(Stats),
    io:format("~p~n", [FlattendStats]).

%% @private take the proplist of stats, each a {name::atom(),
%% stats::proplist()} tuple and create a flat set of stats of
%% {name_stat, val}
flatten_stats(Stats) ->
    flatten_stats(Stats, []).

flatten_stats([], Acc) ->
    lists:flatten(Acc);
flatten_stats([Stat | Stats], Acc) ->
    flatten_stats(Stats, [flatten_stat(Stat) | Acc]).

flatten_stat({Name, Stats}) ->
    lists:foldl(fun({StatName, StatVal}, Acc) ->
                        Flattened = flatten_stat(Name, StatName, StatVal),
                        [Flattened | Acc]
                end,
                [],
                Stats).

-spec flatten_stat(list(), atom(), list() | number()) -> list().
flatten_stat(Parent, Stat, Val) when is_number(Val) ->
    Name = io_lib:format("~w_~w", [Parent, Stat]),
    {list_to_atom(lists:flatten(Name)), Val};
flatten_stat(Parent, Stat, Val) when is_list(Val) ->
    Name = io_lib:format("~w_~w", [Parent, Stat]),
    flatten_stat({list_to_atom(lists:flatten(Name)), Val}).

-ifdef(TEST).

flatten_stats_test() ->
    Stats = [{queue_latency,[{min,0.0},
                     {max,0.0},
                     {arithmetic_mean,0.0},
                     {geometric_mean,0.0},
                     {harmonic_mean,0.0},
                     {median,0.0},
                     {variance,0.0},
                     {standard_deviation,0.0},
                     {skewness,0.0},
                     {kurtosis,0.0},
                     {percentile,[{50,0.0},
                                  {75,0.0},
                                  {90,0.0},
                                  {95,0.0},
                                  {99,0.0},
                                  {999,0.0}]},
                     {histogram,[{0,0}]},
                     {n,0}]},
         {consume,[{count,0},
               {one,0},
               {five,0},
               {fifteen,0},
               {day,0},
               {mean,0.0},
               {acceleration,[{one_to_five,0.0},
                              {five_to_fifteen,0.0},
                              {one_to_fifteen,0.0}]}]}],
    Flat = flatten_stats(Stats),
    [?assertMatch({Name, Val} when is_atom(Name) andalso is_number(Val),
                                   FlatStat) || FlatStat <- Flat].

-endif.
