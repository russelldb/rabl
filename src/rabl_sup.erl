%%%-------------------------------------------------------------------
%% @doc rabl top level supervisor.
%% @end
%%%-------------------------------------------------------------------

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
    ChildSpec = {rabl_consumer, {rabl_consumer, start_link, []},
                 permanent, 5000, worker, [rabl_consumer]},
    {ok, {SupFlags, [ChildSpec]} }.

%%====================================================================
%% Internal functions
%%====================================================================
