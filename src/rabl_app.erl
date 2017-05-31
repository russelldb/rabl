%%%-------------------------------------------------------------------
%% @doc rabl public API
%% @end
%%%-------------------------------------------------------------------

-module(rabl_app).

-behaviour(application).

-export([start/0]).
%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start() ->
    ok = amqp_client:start(),
    application:start(rabl).

start(_StartType, _StartArgs) ->
    lager:info("rabl starting"),
    %% @TODO should be in app defaults? or a macro?
    ConsumerCnt = application:get_env(rabl, consumer_count, 10),
    SupStart = rabl_sup:start_link(ConsumerCnt),
    _Conns = [rabl_channel:start() || _N <- lists:seq(1, 100)],
    SupStart.

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
