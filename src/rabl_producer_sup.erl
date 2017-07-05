%%%-------------------------------------------------------------------
%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 27 Jun 2017 by Russell Brown <russell@wombat.me>
%%%-------------------------------------------------------------------
-module(rabl_producer_sup).

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
    %% @TODO(rdb) think more to justify.

    %% Set like this as default reconnect delay is 50ms. 50ms * 100
    %% restarts is 5000 ms, so this supervisor should never exit
    Intensity = 100,
    PeriodSecs = 5,

    SupFlags = {RestartStrategy, Intensity, PeriodSecs},

    ok = rabl_producer_fsm:load_producer(),
    ProducerSpecs = rabl_producer_fsm:producer_specs(),

    {ok, {SupFlags, ProducerSpecs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
