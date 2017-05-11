%%%-------------------------------------------------------------------
%% @doc rabl top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(rabl_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link(ConsumerCnt) when is_integer(ConsumerCnt)->
    supervisor:start_link({local, ?SERVER}, ?MODULE, [ConsumerCnt]).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([ConsumerCnt]) ->
    SupFlags = {one_for_one, 1, 5},
    ChildSpecs = [{consumer_name(N), {rabl_consumer, start_link, []},
                 permanent, 5000, worker, [rabl_consumer]}
                  || N <- lists:seq(1, ConsumerCnt)],
    {ok, {SupFlags, ChildSpecs} }.

%%====================================================================
%% Internal functions
%%====================================================================
%% @private give the consumers a descriptive name
consumer_name(I) when is_integer(I) ->
    list_to_atom("rabl_consumer_" ++ integer_to_list(I)).
