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
    amqp_client:start(),
    application:start(rabl).

start(_StartType, _StartArgs) ->
    lager:info("rabl starting"),
    %% @TODO should be in app defaults? or a macro?
    {ok, ConsumerCnt} = application:get_env(rabl, consumer_count, {ok, 10}),
    rabl_sup:start_link(ConsumerCnt).

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
