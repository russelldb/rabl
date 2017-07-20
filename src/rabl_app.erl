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
    ok = safe_start_folsom(),
    ok = start_if_not_started(amqp_client),
    application:start(rabl).

%% information about the running status of rabl
status() ->
    ConsumerStatus = rabl_consumer_sup:status(),
    ProducerStatus = rabl_producer_sup:status(),
    ConsumerStatus ++ ProducerStatus.

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

%% @private folsom is started by default by riak, and rabl should be
%% running in riak, but who knows what people will do? Folsom can't
%% handle having start called if it is already started. This function
%% checks if folsom is started before starting it.
safe_start_folsom() ->
    application:ensure_started(bear),
    case whereis(folsom_sup) of
        undefined ->
            folsom:start();
        Pid when is_pid(Pid) ->
            ok
    end.
