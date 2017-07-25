%%%-------------------------------------------------------------------
%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%% record stats for publish, consume for latency tracking
%%% @end
%%% Created : 11 May 2017 by Russell Brown <russell@wombat.me>
%%%-------------------------------------------------------------------
-module(rabl_stat).

-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([
         consume/3,
         get_stats/0,
         params_to_stat_name/1,
         publish/0,
         publish_fail/0,
         riak_put/3,
         start_link/0
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-type stats() :: [stat()].
-type stat() :: {stat_name(), stat_val()}.
-type stat_name() :: atom().
-type stat_val() :: histogram() | counter().
-type histogram() :: proplists:proplist().
-type counter() :: integer().

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, Pid::pid()} | ignore | {error, Error::term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Called when a hook publishes a riak_object
%%%% @end
%%--------------------------------------------------------------------
-spec publish() -> ok.
publish() ->
    folsom_metrics:notify({rabl, publish}, {inc, 1}).

%%--------------------------------------------------------------------
%% @doc
%% Called when a consumer recieves an riak_object
%%%% @end
%%--------------------------------------------------------------------
-spec consume(atom(), erlang:timestamp(), erlang:timestamp()) -> ok.
consume(StatName, PublishTS, ConsumeTS) ->
    case timer:now_diff(ConsumeTS, PublishTS) of
        Qlatency when Qlatency < 0 ->
            lager:warning("Negative queue latency, check clocks synchronized."),
            ok;
        Qlatency ->
            folsom_metrics:notify({rabl, queue_latency}, Qlatency),
            folsom_metrics:notify({rabl, StatName}, Qlatency)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Called when a hook fails to publish a riak object
%%%% @end
%%--------------------------------------------------------------------
-spec publish_fail() -> ok.
publish_fail() ->
    folsom_metrics:notify({{rabl, publish_fail}, 1}).


%%--------------------------------------------------------------------
%% @doc
%% Called when a consumer gets a result from a local riak put
%%%% @end
%%--------------------------------------------------------------------
-spec riak_put(success | fail, erlang:timestamp(), erlang:timestamp()) -> ok.
riak_put(Status, StartTime, EndTime) ->
    case timer:now_diff(EndTime, StartTime) of
        PutLatency when PutLatency < 0 ->
            lager:warning("Negative put latency on local put."),
            ok;
        PutLatency ->
            folsom_metrics:notify({rabl, put_latency}, PutLatency)
    end,
    case Status of
        fail ->
            folsom_metrics:notify({rabl, consume_fail}, {inc, 1});
        success ->
            folsom_metrics:notify({rabl, consume}, {inc, 1})
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get the stats
%%%% @end
%%--------------------------------------------------------------------
-spec get_stats() -> stats().
get_stats() ->
    gen_server:call(?MODULE, get_stats, 5000).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    ok = maybe_create_metrics(),
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(get_stats, _From, State) ->
    Stats = get_all_metrics(),
    Reply = {ok, Stats},
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%% % Internal functions
%% %===================================================================

%% @private set up the metrics needed in folsom
-spec maybe_create_metrics() -> ok | {error, Reason::term()}.
maybe_create_metrics() ->
    Consumers = application:get_env(rabl, consumers, []),
    ConsumerHistograms = [{new_histogram, [{rabl, uri_to_atom(Consumer)}, slide, 10]} ||
                             {_Cnt, Consumer} <- Consumers],
    maybe_create_metrics(ConsumerHistograms ++
                             [
                              {new_histogram, [{rabl, queue_latency}, slide, 10]},
                              {new_histogram, [{rabl, put_latency}, slide, 10]},
                              {new_counter, [{rabl, publish}]},
                              {new_counter, [{rabl, consume}]},
                              {new_counter, [{rabl, publish_fail}]},
                              {new_counter, [{rabl, consume_fail}]}
                             ]).

-spec maybe_create_metrics(Metrics::[{atom(), atom(), integer()}]) ->
                                  ok | {error, Reason::term()}.
maybe_create_metrics([]) ->
    ok;
maybe_create_metrics([{NewFun, Args} | Metrics]) ->
    case apply(folsom_metrics, NewFun, Args) of
        {error, _Name, metric_already_exists} ->
            maybe_create_metrics(Metrics);
        ok ->
            maybe_create_metrics(Metrics);
        OtherError ->
            {error, OtherError}
    end.

-spec get_all_metrics() -> stats().
get_all_metrics() ->
    Metrics =  folsom_metrics:get_metrics(),
    RablStats = lists:foldl(fun({rabl, _Stat}=Name, Acc) ->
                                    Info = folsom_metrics:get_metric_info(Name),
                                    Type = proplists:get_value(type, proplists:get_value(Name, Info)),
                                    [{Name, Type} | Acc];
                               (_, Acc) -> Acc
                            end,
                            [],
                            Metrics),
    get_all_metrics(RablStats, []).

-spec get_all_metrics([{Name::term(), Type::atom()}], Acc::list()) -> stats().
get_all_metrics([], Acc) ->
    Acc;
get_all_metrics([{Name, Type} | Stats], Acc) ->
    get_all_metrics(Stats, [get_stat(Name, Type)|Acc]).

-spec get_stat(Name::term(), Type::atom()) -> stat().
get_stat({rabl, Stat}=Name, histogram) ->
    {Stat, folsom_metrics:get_histogram_statistics(Name)};
get_stat({rabl, Stat}=Name, _Type) ->
    {Stat, folsom_metrics:get_metric_value(Name)}.

-spec uri_to_atom(string()) -> atom().
uri_to_atom(URI) ->
    {ok, Params} = rabl_amqp:parse_uri(URI),
    params_to_stat_name(Params).

-spec params_to_stat_name(rabl_amqp:amqp_connection_params()) -> atom().
params_to_stat_name(Params) ->
    Host = rabl_amqp:host(Params),
    Host2 = re:replace(Host, "\\.", "_", [global, {return, list}]),
    list_to_atom("queue_"++Host2).

%%%===================================================================
%% TESTS
%%%===================================================================
-ifdef(TEST).

metrics_test() ->
    application:ensure_started(folsom),
    {ok, Pid} = start_link(),
    unlink(Pid),
    MonRef = monitor(process, Pid),
    assert_stats_created(),

    add_stats(),

    exit(Pid, kill),

    receive
        {'DOWN', MonRef, process, Pid, killed} ->
            %% Wait for dead before restarting
            ok
    end,

    {ok, _Pid2} = start_link(),

    assert_stats_created(),
    assert_stats_survived_crash().

assert_stats_created() ->
    Expected = lists:sort([{rabl, Name} || Name <- [consume,publish,queue_latency,put_latency,publish_fail,consume_fail]]),
    ?assertEqual(Expected, lists:sort(folsom_metrics:get_metrics())).

add_stats() ->
    [folsom_metrics:notify({rabl, Name}, {inc, 1000}) || Name <- [consume, consume_fail, publish_fail, publish]].

assert_stats_survived_crash() ->
    {ok, Stats} = get_stats(),
    [?assertMatch({Name,  1000}, proplists:lookup(Name, Stats)) || Name <- [consume, consume_fail, publish_fail, publish]].

-endif.
