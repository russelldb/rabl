%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created :  2 May 2017 by Russell Brown <russell@wombat.me>
-module(rabl_util).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-compile(export_all).

%% QAD add hook
-spec add_hook(Bucket::binary() | {binary(), binary()}) -> ok.
add_hook(Bucket) ->
    {ok, C} = rabl_riak:client_new(),
    rabl_riak:set_bucket(C, Bucket,[{postcommit, [{struct,[{<<"mod">>,<<"rabl_hook">>},{<<"fun">>, <<"rablicate">>}]}]}]).

%% @doc get bucket props.
-spec get_bucket(Bucket::binary()) -> proplists:proplist().
get_bucket(Bucket) ->
    {ok, C} = rabl_riak:client_new(),
    rabl_riak:get_bucket(C, Bucket).

load() ->
    application:load(rabl).

%% convenience local put
-spec put(Bucket::binary() | {binary(), binary()},
          Key::binary(), Val::binary()) -> ok.
put(Bucket, Key, Value) ->
    {ok, C} = rabl_riak:client_new(),
    %% @TODO: dependancy on riak_kv/riak_object
    Obj = riak_object:new(Bucket, Key, Value),
    ok = rabl_riak:client_put(C, Obj, []),
    ok.

%% sets up exchange, queue, bindings locally for a smoke test NOTE:
%% run before starting the app.
setup_local_smoke() ->
    application:load(rabl),
    RablEnv = application:get_all_env(rabl),
    ClusterName = proplists:get_value(cluster_name, RablEnv),
    SinkQueue = proplists:get_value(sink_queue, RablEnv),
    Producers = proplists:get_value(producers, RablEnv),
    Consumers = proplists:get_value(consumers, RablEnv),
    ok = setup_queues(ClusterName, Producers),
    ok = setup_queues(SinkQueue, Consumers).

setup_queues(_Queue, []) ->
    ok;
setup_queues(Queue, [{_Cnt, URI} | Rest]) ->
    setup_queue(Queue, URI),
    setup_queues(Queue, Rest).

setup_queue(Queue, AMQPURI) ->
    {ok, AMQPParams} = rabl_amqp:parse_uri(AMQPURI),
    {ok, Connection}  = rabl_amqp:connection_start(AMQPParams),
    io:format("connected~n"),
    {ok, Channel} = rabl_amqp:channel_open(Connection),
    io:format("got channel~n"),
    {ok, Queue} = rabl_amqp:queue_create(Channel, Queue),
    io:format("made queue~n"),
    {ok, Exchange} = rabl_amqp:exchange_create(Channel, Queue),
    io:format("made exchange~n"),
    {ok, RoutingKey} = rabl_amqp:bind(Channel, Exchange, Queue, Queue),
    RoutingKey.

get_flat_stats() ->
    {ok, Stats} = rabl_stat:get_stats(),
    flatten_stats(Stats).

%% @private take the proplist of stats, each a {name::atom(),
%% stats::proplist()} tuple and create a flat set of stats of
%% {name_stat, val}
flatten_stats(Stats) ->
    flatten_stats(Stats, []).

flatten_stats([], Acc) ->
    lists:flatten(Acc);
flatten_stats([Stat | Stats], Acc) ->
    flatten_stats(Stats, [flatten_stat(Stat) | Acc]).

flatten_stat({Name, Stat}) when is_number(Stat) ->
    [flatten_stat(Name, count, Stat)];
flatten_stat({Name, Stats}) when is_list(Stats) ->
    %% @TODO better filter on stats to show
    [flatten_stat(Name, StatName, StatVal) || {StatName, StatVal} <- Stats,
                                              lists:member(StatName,
                                                           [percentile, 50, 75, 90, 95, 99, 999,
                                                            count, median, min, max])].

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
