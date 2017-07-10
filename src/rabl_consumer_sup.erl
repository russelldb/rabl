%%%-------------------------------------------------------------------
%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 10 July 2017 by Russell Brown <russell@wombat.me>
%%%-------------------------------------------------------------------
-module(rabl_consumer_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_link() -> {ok, pid()} | {error, Error::term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================


%% @private use the rabl application config/env vars to start a `N'
%% supervised producer fsms.
init([]) ->
    RestartStrategy = one_for_one,
    Intensity = 10,
    PeriodSecs = 5,

    SupFlags = {RestartStrategy, Intensity, PeriodSecs},

    ConsumerSpecs = consumer_specs(),
    {ok, {SupFlags, ConsumerSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
consumer_specs() ->
    {ok, Consumers} = application:get_env(rabl, consumers),
    {_Cnt, ChildSpecs} = lists:foldl(fun(Consumer, {ChildCnt, SpecsAcc}) ->
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
