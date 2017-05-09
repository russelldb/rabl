%%%-------------------------------------------------------------------
%% @doc rabl public API
%% @end
%%%-------------------------------------------------------------------

-module(rabl_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    %% check that we can connect to the riak node and all that
    RiakNode = application:get_env(rabl, riak_node),
    RiakCookie = application:get_env(rabl, riak_cookie),
    erlang:set_cookie(RiakNode, RiakCookie),
    true = net_kernel:connect_node(RiakNode),
    rabl_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
