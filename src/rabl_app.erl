%%%-------------------------------------------------------------------
%% @doc rabl public API
%% @end
%%%-------------------------------------------------------------------

-module(rabl_app).

-behaviour(application).

-export([start/0, status/0]).
%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start() ->
    start_if_not_started(lager),
    %% I have no idea what is going on with bear, it's not even an
    %% app.
    ok = start_if_not_started(application, bear),
    ok = start_if_not_started(folsom),
    ok = start_if_not_started(amqp_client),
    application:start(rabl).

%% information about the running status of rabl
status() ->
    ok.

start(_StartType, _StartArgs) ->
    lager:info("rabl starting"),
    rabl_sup:start_link().


%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
start_if_not_started(App) ->
    case App:start() of
        {error,{already_started, App}} ->
            ok;
        ok ->
            ok;
        OtherError ->
            OtherError
    end.

start_if_not_started(application, App) ->
    case application:start(App) of
        {error,{already_started, App}} ->
            ok;
        ok ->
            ok;
        OtherError ->
            OtherError
    end.
