%%%-------------------------------------------------------------------
%% @doc rabl top level supervisor. Starts the consumer gen_servers
%% @end
%% %-------------------------------------------------------------------

-module(rabl_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    SupFlags = {one_for_one, 1, 5},
    RablStat = {rabl_stat, {rabl_stat, start_link, []},
                 transient, 5000, worker, [rabl_stat]},
    RablProducer = {rabl_producer, {rabl_producer, start_link, []},
                    permanent, 5000, worker, [rabl_producer]},
    ConsumerSpecs = consumer_specs(),
    {ok, {SupFlags, ConsumerSpecs ++ [RablStat, RablProducer]}}.

%%====================================================================
%% Internal functions
%%====================================================================

consumer_specs() ->
    {ok, Consumers} = application:get_env(rabl, consumers),
    ChildSpecs = lists:foldl(fun(Consumer, {ChildCnt, SpecsAcc}) ->
                                     {ChildCnt2, Specs} = consumers(Consumer, ChildCnt),
                                     {ChildCnt2, [Specs | SpecsAcc]}
                             end,
                             {0, []},
                             Consumers),
    lists:flatten(ChildSpecs).

consumers({Cnt, URI}, NameCounter) ->
    lists:foldl(fun(_Seq, {NameCntAcc, SpecAcc}) ->
                        {NameCntAcc+1, [child_spec(NameCntAcc, URI) | SpecAcc]}
                end,
                {NameCounter, []},
                lists:seq(1, Cnt)).

child_spec(ChildCnt, URI) ->
    {consumer_name(ChildCnt),
     {rabl_consumer_fsm, start_link, [URI]},
     permanent, 5000, worker, [rabl_consumer_fsm]}.

%% @private give the consumers a descriptive name
-spec consumer_name(non_neg_integer()) -> atom().
consumer_name(I) when is_integer(I) ->
    list_to_atom("rabl_consumer_" ++ integer_to_list(I)).
