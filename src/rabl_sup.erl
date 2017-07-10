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
    SupFlags = {one_for_one, 10, 5},
    RablStat = {rabl_stat, {rabl_stat, start_link, []},
                transient, 5000, worker, [rabl_stat]},
    RablProducerSup = {rabl_producer_sup, {rabl_producer_sup, start_link, []},
                       permanent, 5000, supervisor, [rabl_producer_sup]},

    RablConsumerSup = {rabl_consumer_sup, {rabl_consumer_sup, start_link, []},
                       permanent, 5000, supervisor, [rabl_consumer_sup]},

    {ok, {SupFlags, [RablStat, RablProducerSup, RablConsumerSup]}}.

%%====================================================================
%% Internal functions
%%====================================================================

